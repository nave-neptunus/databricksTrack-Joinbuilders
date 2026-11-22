use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use parquet::basic::{Compression, Encoding};
use parquet::column::page::{CompressedPage, Page, PageWriter};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, ParquetMetaDataWriter, RowGroupMetaData,
};
use parquet::file::writer::{SerializedPageWriter, TrackedWrite};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::SchemaDescriptor;

use super::UserDay;

const SCHEMA: &str = "
    message user_daily {
        REQUIRED BYTE_ARRAY user_id (UTF8);
        REQUIRED INT32 day (DATE);
        REQUIRED INT64 sessions;
        REQUIRED INT64 events;
        REQUIRED INT64 revenue_cents;
        OPTIONAL BYTE_ARRAY top_category (UTF8);
    }
";
const GROUP_ROWS: u32 = 65_536;
const MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const DEFINITION_SPACE: usize = GROUP_ROWS as usize / 8 + 3;

pub(super) fn schema() -> Arc<SchemaDescriptor> {
    Arc::new(SchemaDescriptor::new(Arc::new(
        parse_message_type(SCHEMA).unwrap(),
    )))
}

/// Append final values in their on-disk PLAIN representation as each day closes.
pub(super) struct UserParquet {
    file: TrackedWrite<BufWriter<File>>,
    schema: Arc<SchemaDescriptor>,
    row_groups: Vec<RowGroupMetaData>,
    columns: [BytesMut; 6],
    present: Vec<u8>,
    rows: u32,
    nulls: u32,
}

impl UserParquet {
    pub(super) fn new(path: String, schema: Arc<SchemaDescriptor>) -> Self {
        let mut file = TrackedWrite::new(BufWriter::with_capacity(
            1 << 20,
            File::create(path).unwrap(),
        ));
        file.write_all(b"PAR1").unwrap();
        let mut columns: [BytesMut; 6] = Default::default();
        columns[5].resize(DEFINITION_SPACE, 0);
        Self {
            file,
            schema,
            row_groups: Vec::new(),
            columns,
            present: Vec::new(),
            rows: 0,
            nulls: 0,
        }
    }

    pub(super) fn append(&mut self, name: &[u8], day: &UserDay, category: Option<&[u8]>) {
        let [names, days, sessions, events, revenue, categories] = &mut self.columns;
        plain_string(names, name);
        days.put_i32_le(day.day as i32);
        sessions.put_i64_le(day.sessions);
        events.put_i64_le(day.events);
        revenue.put_i64_le(day.revenue);

        let bit = self.rows % 8;
        if bit == 0 {
            self.present.push(0);
        }
        if let Some(category) = category {
            *self.present.last_mut().unwrap() |= 1 << bit;
            plain_string(categories, category);
        } else {
            self.nulls += 1;
        }
        self.rows += 1;
        if self.rows == GROUP_ROWS || names.len().max(categories.len()) >= MAX_BUFFER_BYTES {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.rows == 0 {
            return;
        }
        let file_offset = self.file.bytes_written() as i64;
        let mut group = RowGroupMetaData::builder(self.schema.clone())
            .set_num_rows(i64::from(self.rows))
            .set_file_offset(file_offset);
        for (index, (column, descriptor)) in self
            .columns
            .iter_mut()
            .zip(self.schema.columns())
            .enumerate()
        {
            let optional = index == 5;
            let (start, nulls) = if optional {
                let start = definition_levels(column, &self.present);
                (start, self.nulls)
            } else {
                (0, 0)
            };
            let body = std::mem::take(column).freeze();
            let size = body.len() - start;
            let page = Page::DataPageV2 {
                buf: body.slice(start..),
                num_values: self.rows,
                encoding: Encoding::PLAIN,
                num_nulls: nulls,
                num_rows: self.rows,
                def_levels_byte_len: if optional {
                    (DEFINITION_SPACE - start) as u32
                } else {
                    0
                },
                rep_levels_byte_len: 0,
                is_compressed: false,
                statistics: None,
            };
            let spec = SerializedPageWriter::new(&mut self.file)
                .write_page(CompressedPage::new(page, size))
                .unwrap();
            *column = body.try_into_mut().expect("finished page owns no buffer");
            column.clear();
            if optional {
                column.resize(DEFINITION_SPACE, 0);
            }

            let encodings = if optional {
                vec![Encoding::PLAIN, Encoding::RLE]
            } else {
                vec![Encoding::PLAIN]
            };
            let metadata = ColumnChunkMetaData::builder(descriptor.clone())
                .set_encodings(encodings)
                .set_compression(Compression::UNCOMPRESSED)
                .set_num_values(i64::from(self.rows))
                .set_total_compressed_size(spec.compressed_size as i64)
                .set_total_uncompressed_size(spec.uncompressed_size as i64)
                .set_data_page_offset(spec.offset as i64)
                .build()
                .unwrap();
            group = group.add_column_metadata(metadata);
        }
        let group = group.set_total_byte_size(self.file.bytes_written() as i64 - file_offset);
        self.row_groups.push(group.build().unwrap());
        self.present.clear();
        self.rows = 0;
        self.nulls = 0;
    }

    pub(super) fn close(mut self) {
        self.flush();
        let rows = self.row_groups.iter().map(RowGroupMetaData::num_rows).sum();
        let file = FileMetaData::new(2, rows, None, None, self.schema, None);
        let metadata = ParquetMetaData::new(file, self.row_groups);
        // Page offsets are already absolute and there are no page indexes to write.
        ParquetMetaDataWriter::new(&mut self.file, &metadata)
            .finish()
            .unwrap();
        self.file.flush().unwrap();
    }
}

fn plain_string(buf: &mut BytesMut, value: &[u8]) {
    buf.put_u32_le(u32::try_from(value.len()).unwrap());
    buf.extend_from_slice(value);
}

/// Encode one bit-packed hybrid run into the reserved space before the values.
fn definition_levels(column: &mut BytesMut, bits: &[u8]) -> usize {
    let mut header = (bits.len() as u32) * 2 + 1;
    let mut prefix = [0u8; 3];
    let mut len = 0;
    while header >= 128 {
        prefix[len] = header as u8 | 0x80;
        header >>= 7;
        len += 1;
    }
    prefix[len] = header as u8;
    len += 1;
    let start = DEFINITION_SPACE - bits.len() - len;
    column[start..start + len].copy_from_slice(&prefix[..len]);
    column[start + len..DEFINITION_SPACE].copy_from_slice(bits);
    start
}

const CAMPAIGN_SCHEMA: &str = "
    message campaign_daily {
        REQUIRED BYTE_ARRAY campaign_id (UTF8);
        REQUIRED BYTE_ARRAY channel (UTF8);
        REQUIRED INT32 day (DATE);
        REQUIRED INT64 sessions;
        REQUIRED INT64 purchasing_sessions;
        REQUIRED INT64 revenue_cents;
        REQUIRED INT64 assisted_sessions;
    }
";

/// One campaign_daily row: campaign, channel, day, then sessions, purchasing
/// sessions, revenue and assisted sessions.
pub(super) type CampaignRow<'a> = (&'a [u8], &'a [u8], i32, [i64; 4]);

/// Write the whole (small) campaign_daily table as one PLAIN, uncompressed row group.
pub(super) fn write_campaign(path: &str, rows: &[CampaignRow]) {
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(
        parse_message_type(CAMPAIGN_SCHEMA).unwrap(),
    )));
    let mut file = TrackedWrite::new(BufWriter::with_capacity(1 << 16, File::create(path).unwrap()));
    file.write_all(b"PAR1").unwrap();
    let mut row_groups = Vec::new();
    if !rows.is_empty() {
        let mut columns: [BytesMut; 7] = Default::default();
        for &(campaign, channel, day, values) in rows {
            let [c, h, d, a, b, r, s] = &mut columns;
            plain_string(c, campaign);
            plain_string(h, channel);
            d.put_i32_le(day);
            for (column, value) in [a, b, r, s].into_iter().zip(values) {
                column.put_i64_le(value);
            }
        }
        let num_rows = rows.len() as u32;
        let file_offset = file.bytes_written() as i64;
        let mut group = RowGroupMetaData::builder(schema.clone())
            .set_num_rows(i64::from(num_rows))
            .set_file_offset(file_offset);
        for (column, descriptor) in columns.into_iter().zip(schema.columns()) {
            let body = column.freeze();
            let size = body.len();
            let page = Page::DataPageV2 {
                buf: body,
                num_values: num_rows,
                encoding: Encoding::PLAIN,
                num_nulls: 0,
                num_rows,
                def_levels_byte_len: 0,
                rep_levels_byte_len: 0,
                is_compressed: false,
                statistics: None,
            };
            let spec = SerializedPageWriter::new(&mut file)
                .write_page(CompressedPage::new(page, size))
                .unwrap();
            let metadata = ColumnChunkMetaData::builder(descriptor.clone())
                .set_encodings(vec![Encoding::PLAIN])
                .set_compression(Compression::UNCOMPRESSED)
                .set_num_values(i64::from(num_rows))
                .set_total_compressed_size(spec.compressed_size as i64)
                .set_total_uncompressed_size(spec.uncompressed_size as i64)
                .set_data_page_offset(spec.offset as i64)
                .build()
                .unwrap();
            group = group.add_column_metadata(metadata);
        }
        let group = group.set_total_byte_size(file.bytes_written() as i64 - file_offset);
        row_groups.push(group.build().unwrap());
    }
    let total = row_groups.iter().map(RowGroupMetaData::num_rows).sum();
    let meta = FileMetaData::new(2, total, None, None, schema, None);
    let metadata = ParquetMetaData::new(meta, row_groups);
    ParquetMetaDataWriter::new(&mut file, &metadata).finish().unwrap();
    file.flush().unwrap();
}
