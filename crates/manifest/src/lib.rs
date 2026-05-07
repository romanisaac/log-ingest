use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::path::Path;

/// Metadata for a single Parquet file registered in the manifest.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub id: i64,
    pub path: String,
    pub tier: String,
    pub service: String,
    pub time_bucket: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub size_bytes: i64,
    pub record_count: i64,
    pub state: String,
    pub min_kafka_offset: i64,
    pub max_kafka_offset: i64,
}

/// Parameters needed to register a new file.
pub struct FlushMeta {
    pub path: String,
    pub service: String,
    pub time_bucket: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub size_bytes: i64,
    pub record_count: i64,
    pub min_kafka_offset: i64,
    pub max_kafka_offset: i64,
}

/// Per-bucket statistics for compaction threshold evaluation.
pub struct BucketStats {
    pub service: String,
    pub time_bucket: String,
    pub file_count: usize,
    pub total_records: i64,
    /// Estimated duplicate density in [0.0, 1.0].
    /// 0.0 means no duplicates detected; 1.0 means all records are duplicates.
    pub duplicate_density: f64,
}

/// SQLite-backed manifest catalog.
pub struct Manifest {
    conn: Connection,
}

impl Manifest {
    /// Open (or create) the manifest database at the given path.
    pub fn open(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path).context("open manifest db")?;

        // WAL mode for concurrent reads.
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .context("set WAL mode")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS files (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                path              TEXT NOT NULL UNIQUE,
                tier              TEXT NOT NULL DEFAULT 'hot',
                service           TEXT NOT NULL,
                time_bucket       TEXT NOT NULL,
                min_ts            INTEGER NOT NULL,
                max_ts            INTEGER NOT NULL,
                size_bytes        INTEGER NOT NULL,
                record_count      INTEGER NOT NULL,
                state             TEXT NOT NULL DEFAULT 'active',
                min_kafka_offset  INTEGER NOT NULL,
                max_kafka_offset  INTEGER NOT NULL,
                superseded_at     INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_files_covering
                ON files (service, time_bucket, min_ts, max_ts);",
        )
        .context("create schema")?;

        // Migrate existing databases that pre-date the superseded_at column.
        // ALTER TABLE ADD COLUMN fails with "duplicate column name" if the column
        // already exists — that error is intentionally ignored.
        let _ = conn.execute_batch(
            "ALTER TABLE files ADD COLUMN superseded_at INTEGER NOT NULL DEFAULT 0;",
        );

        Ok(Manifest { conn })
    }

    /// Register a newly flushed Parquet file.
    pub fn commit_flush(&mut self, meta: &FlushMeta) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO files
                (path, service, time_bucket, min_ts, max_ts,
                 size_bytes, record_count, min_kafka_offset, max_kafka_offset)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                meta.path,
                meta.service,
                meta.time_bucket,
                meta.min_ts,
                meta.max_ts,
                meta.size_bytes,
                meta.record_count,
                meta.min_kafka_offset,
                meta.max_kafka_offset,
            ],
        )
        .context("insert file entry")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Return active hot-tier files whose max_ts is strictly before `cutoff_ns`.
    pub fn hot_files_older_than(&self, cutoff_ns: i64) -> Result<Vec<FileEntry>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, path, tier, service, time_bucket, min_ts, max_ts,
                        size_bytes, record_count, state, min_kafka_offset, max_kafka_offset
                 FROM files WHERE state = 'active' AND tier = 'hot' AND max_ts < ?1",
            )
            .context("prepare hot_files_older_than")?;
        let rows = stmt
            .query_map(params![cutoff_ns], map_row)
            .context("query hot_files_older_than")?
            .collect::<std::result::Result<_, _>>()
            .context("collect rows")?;
        Ok(rows)
    }

    /// Promote a file to the cold tier, updating its path to the object-store URI.
    /// This is atomic: tier and path change in a single UPDATE.
    pub fn set_cold(&mut self, id: i64, cold_path: &str) -> Result<()> {
        self.conn
            .execute(
                "UPDATE files SET tier = 'cold', path = ?1 WHERE id = ?2",
                params![cold_path, id],
            )
            .context("set_cold")?;
        Ok(())
    }

    /// Count active files grouped by tier. Returns (hot_count, cold_count).
    pub fn count_active_by_tier(&self) -> Result<(u64, u64)> {
        let mut stmt = self
            .conn
            .prepare("SELECT tier, COUNT(*) FROM files WHERE state = 'active' GROUP BY tier")
            .context("prepare count_active_by_tier")?;
        let mut hot = 0u64;
        let mut cold = 0u64;
        let rows = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))
            .context("query count_active_by_tier")?;
        for row in rows {
            let (tier, count) = row.context("read tier count row")?;
            match tier.as_str() {
                "hot" => hot = count as u64,
                "cold" => cold = count as u64,
                _ => {}
            }
        }
        Ok((hot, cold))
    }

    /// Return (service, time_bucket) pairs that have at least `min_files` hot active files.
    pub fn compactable_buckets(&self, min_files: usize) -> Result<Vec<(String, String)>> {
        let min = min_files as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT service, time_bucket FROM files
                 WHERE state = 'active' AND tier = 'hot'
                 GROUP BY service, time_bucket
                 HAVING COUNT(*) >= ?1",
            )
            .context("prepare compactable_buckets")?;
        let rows = stmt
            .query_map(params![min], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .context("query compactable_buckets")?
            .collect::<std::result::Result<_, _>>()
            .context("collect rows")?;
        Ok(rows)
    }

    /// Return all active hot files for a given (service, time_bucket).
    pub fn active_hot_files_for_bucket(
        &self,
        service: &str,
        time_bucket: &str,
    ) -> Result<Vec<FileEntry>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, path, tier, service, time_bucket, min_ts, max_ts,
                        size_bytes, record_count, state, min_kafka_offset, max_kafka_offset
                 FROM files
                 WHERE state = 'active' AND tier = 'hot'
                   AND service = ?1 AND time_bucket = ?2",
            )
            .context("prepare active_hot_files_for_bucket")?;
        let rows = stmt
            .query_map(params![service, time_bucket], map_row)
            .context("query active_hot_files_for_bucket")?
            .collect::<std::result::Result<_, _>>()
            .context("collect rows")?;
        Ok(rows)
    }

    /// Atomic compaction swap: insert new compacted files and mark old files superseded.
    /// No window where data is missing — old files remain active until commit.
    pub fn swap_compacted(&mut self, old_ids: &[i64], new_files: &[FlushMeta]) -> Result<()> {
        let tx = self.conn.transaction().context("begin compaction transaction")?;
        for meta in new_files {
            tx.execute(
                "INSERT INTO files
                    (path, service, time_bucket, min_ts, max_ts,
                     size_bytes, record_count, min_kafka_offset, max_kafka_offset)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    meta.path,
                    meta.service,
                    meta.time_bucket,
                    meta.min_ts,
                    meta.max_ts,
                    meta.size_bytes,
                    meta.record_count,
                    meta.min_kafka_offset,
                    meta.max_kafka_offset,
                ],
            )
            .context("insert compacted file")?;
        }
        for &id in old_ids {
            tx.execute(
                "UPDATE files SET state = 'superseded', superseded_at = unixepoch() WHERE id = ?1",
                params![id],
            )
            .context("mark file superseded")?;
        }
        tx.commit().context("commit compaction swap")
    }

    /// Per-bucket statistics used to evaluate compaction thresholds.
    pub fn hot_bucket_stats(&self) -> Result<Vec<BucketStats>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT service, time_bucket,
                        COUNT(*) AS file_count,
                        SUM(record_count) AS total_records,
                        MAX(max_kafka_offset) - MIN(min_kafka_offset) + 1 AS offset_span
                 FROM files
                 WHERE state = 'active' AND tier = 'hot'
                 GROUP BY service, time_bucket",
            )
            .context("prepare hot_bucket_stats")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .context("query hot_bucket_stats")?;
        let mut result = Vec::new();
        for row in rows {
            let (service, time_bucket, file_count, total_records, offset_span) =
                row.context("read bucket stats row")?;
            // Proxy for duplicate density: records above the unique-offset span are
            // likely duplicates. This underestimates with multiple Kafka partitions
            // but is a cheap, index-free signal.
            let duplicate_density = if total_records > 0 && offset_span > 0 {
                ((total_records - offset_span).max(0) as f64) / total_records as f64
            } else {
                0.0
            };
            result.push(BucketStats {
                service,
                time_bucket,
                file_count: file_count as usize,
                total_records,
                duplicate_density,
            });
        }
        Ok(result)
    }

    /// Return paths of all active files, optionally filtered by service.
    pub fn active_files(&self, service: Option<&str>) -> Result<Vec<FileEntry>> {
        let mut stmt = if let Some(svc) = service {
            let mut s = self
                .conn
                .prepare(
                    "SELECT id, path, tier, service, time_bucket, min_ts, max_ts,
                            size_bytes, record_count, state, min_kafka_offset, max_kafka_offset
                     FROM files WHERE state = 'active' AND service = ?1",
                )
                .context("prepare active_files query")?;
            let rows = s
                .query_map(params![svc], map_row)
                .context("query active_files")?;
            return rows.collect::<std::result::Result<_, _>>().context("collect rows");
        } else {
            self.conn
                .prepare(
                    "SELECT id, path, tier, service, time_bucket, min_ts, max_ts,
                            size_bytes, record_count, state, min_kafka_offset, max_kafka_offset
                     FROM files WHERE state = 'active'",
                )
                .context("prepare active_files query")?
        };

        let rows = stmt
            .query_map([], map_row)
            .context("query active_files")?;
        rows.collect::<std::result::Result<_, _>>().context("collect rows")
    }

    /// Return active files older than the given retention cutoffs (nanosecond timestamps).
    /// Hot and cold tiers may have different retention windows.
    pub fn expired_active_files(
        &self,
        hot_cutoff_ns: i64,
        cold_cutoff_ns: i64,
    ) -> Result<Vec<FileEntry>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, path, tier, service, time_bucket, min_ts, max_ts,
                        size_bytes, record_count, state, min_kafka_offset, max_kafka_offset
                 FROM files
                 WHERE state = 'active'
                   AND ((tier = 'hot'  AND max_ts < ?1)
                     OR (tier = 'cold' AND max_ts < ?2))",
            )
            .context("prepare expired_active_files")?;
        let rows = stmt
            .query_map(params![hot_cutoff_ns, cold_cutoff_ns], map_row)
            .context("query expired_active_files")?
            .collect::<std::result::Result<_, _>>()
            .context("collect rows")?;
        Ok(rows)
    }

    /// Return superseded files whose superseded_at timestamp is before `grace_cutoff_unix_secs`.
    /// Files with superseded_at = 0 (pre-migration) are excluded to avoid mass-deleting
    /// files that were superseded before the column existed.
    pub fn superseded_files_past_grace(
        &self,
        grace_cutoff_unix_secs: i64,
    ) -> Result<Vec<FileEntry>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, path, tier, service, time_bucket, min_ts, max_ts,
                        size_bytes, record_count, state, min_kafka_offset, max_kafka_offset
                 FROM files
                 WHERE state = 'superseded'
                   AND superseded_at > 0
                   AND superseded_at < ?1",
            )
            .context("prepare superseded_files_past_grace")?;
        let rows = stmt
            .query_map(params![grace_cutoff_unix_secs], map_row)
            .context("query superseded_files_past_grace")?
            .collect::<std::result::Result<_, _>>()
            .context("collect rows")?;
        Ok(rows)
    }

    /// Remove a file entry from the manifest by id.
    pub fn delete_file(&mut self, id: i64) -> Result<()> {
        self.conn
            .execute("DELETE FROM files WHERE id = ?1", params![id])
            .context("delete_file")?;
        Ok(())
    }
}

fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileEntry> {
    Ok(FileEntry {
        id: row.get(0)?,
        path: row.get(1)?,
        tier: row.get(2)?,
        service: row.get(3)?,
        time_bucket: row.get(4)?,
        min_ts: row.get(5)?,
        max_ts: row.get(6)?,
        size_bytes: row.get(7)?,
        record_count: row.get(8)?,
        state: row.get(9)?,
        min_kafka_offset: row.get(10)?,
        max_kafka_offset: row.get(11)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn insert_file(m: &mut Manifest, state: &str, max_ts: i64, superseded_at: i64) -> i64 {
        m.conn.execute(
            "INSERT INTO files
                (path, tier, service, time_bucket, min_ts, max_ts,
                 size_bytes, record_count, state, min_kafka_offset, max_kafka_offset, superseded_at)
             VALUES (?, 'hot', 'svc', '2024-01-01-00', 0, ?, 0, 0, ?, 0, 0, ?)",
            rusqlite::params![
                format!("/tmp/fake-{}.parquet", uuid::Uuid::new_v4()),
                max_ts, state, superseded_at
            ],
        ).unwrap();
        m.conn.last_insert_rowid()
    }

    #[test]
    fn expired_active_files_returns_only_expired() {
        let dir = tempdir().unwrap();
        let mut m = Manifest::open(&dir.path().join("m.db")).unwrap();

        let old_id = insert_file(&mut m, "active", 1_000, 0);       // ancient
        let _new_id = insert_file(&mut m, "active", i64::MAX, 0);    // far future

        let expired = m.expired_active_files(1_000_000_000, i64::MAX).unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, old_id);
    }

    #[test]
    fn superseded_files_past_grace_excludes_recent_and_legacy() {
        let dir = tempdir().unwrap();
        let mut m = Manifest::open(&dir.path().join("m.db")).unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;

        let old_id = insert_file(&mut m, "superseded", 0, now - 3600); // 1h ago
        let _recent = insert_file(&mut m, "superseded", 0, now);        // just now
        let _legacy = insert_file(&mut m, "superseded", 0, 0);          // pre-migration default

        let grace_cutoff = now - 600; // 10-minute grace
        let eligible = m.superseded_files_past_grace(grace_cutoff).unwrap();
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].id, old_id);
    }

    #[test]
    fn delete_file_removes_row() {
        let dir = tempdir().unwrap();
        let mut m = Manifest::open(&dir.path().join("m.db")).unwrap();
        let id = insert_file(&mut m, "active", 1000, 0);
        m.delete_file(id).unwrap();
        let count: i64 = m.conn
            .query_row("SELECT COUNT(*) FROM files WHERE id = ?1", rusqlite::params![id], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}
