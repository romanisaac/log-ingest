use event_schema::LogEvent;

pub const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;

pub struct BatchBuffer {
    events: Vec<LogEvent>,
    current_bytes: usize,
    max_bytes: usize,
}

impl BatchBuffer {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            events: Vec::new(),
            current_bytes: 0,
            max_bytes,
        }
    }

    /// Push an event. Returns the flushed batch if the size threshold is reached.
    pub fn push(&mut self, event: LogEvent) -> Option<Vec<LogEvent>> {
        self.current_bytes += event.estimated_byte_size();
        self.events.push(event);
        if self.current_bytes >= self.max_bytes {
            Some(self.drain())
        } else {
            None
        }
    }

    /// Drain all buffered events, resetting the buffer.
    pub fn drain(&mut self) -> Vec<LogEvent> {
        self.current_bytes = 0;
        std::mem::take(&mut self.events)
    }

    /// Fraction of max_bytes currently buffered, clamped to [0.0, 1.0].
    pub fn occupancy(&self) -> f32 {
        (self.current_bytes as f32 / self.max_bytes as f32).min(1.0)
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_schema::{AttributeValue, Level};
    use std::collections::HashMap;

    fn make_event(message_size: usize) -> LogEvent {
        LogEvent {
            timestamp: 1_700_000_000_000_000_000,
            level: Level::Info,
            service: "svc".to_string(),
            message: "x".repeat(message_size),
            kafka_partition: 0,
            kafka_offset: 0,
            attributes: HashMap::new(),
        }
    }

    #[test]
    fn size_trigger_fires_exactly_once() {
        // Each event's estimated size: 8 (ts) + 4 (level) + 3 (svc) + message_size + 4 (part) + 8 (offset) = 27 + message_size
        // Use max_bytes = 100; push events until we cross it, then verify flush happened.
        let mut buf = BatchBuffer::new(100);

        // Push events of size 27 + 40 = 67 bytes each.
        // First push: 67 bytes — no flush.
        let result = buf.push(make_event(40));
        assert!(result.is_none());
        assert_eq!(buf.len(), 1);

        // Second push: 134 bytes total — crosses 100, flush triggered.
        let result = buf.push(make_event(40));
        let batch = result.expect("expected flush on second push");
        assert_eq!(batch.len(), 2);

        // Buffer should be empty after flush.
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn occupancy_tracks_fill_level() {
        let mut buf = BatchBuffer::new(100);
        assert_eq!(buf.occupancy(), 0.0);

        // One event of size 50 bytes → occupancy = 0.5
        // 8 + 4 + 3 + 27 + 4 + 8 = ... let's use message_size such that total = 50
        // 27 + message_size = 50 → message_size = 23
        buf.push(make_event(23));
        let occ = buf.occupancy();
        assert!(occ > 0.0 && occ <= 1.0, "occupancy={occ}");
    }

    #[test]
    fn buffer_empty_after_drain() {
        let mut buf = BatchBuffer::new(DEFAULT_MAX_BYTES);
        buf.push(make_event(10));
        buf.push(make_event(10));
        assert!(!buf.is_empty());

        let batch = buf.drain();
        assert_eq!(batch.len(), 2);
        assert!(buf.is_empty());
        assert_eq!(buf.occupancy(), 0.0);
    }

    #[test]
    fn oversized_event_flushes_immediately() {
        // An event larger than max_bytes triggers a flush on that same push and
        // resets the buffer to empty. Occupancy never gets stuck above 1.0.
        let mut buf = BatchBuffer::new(10);
        let batch = buf.push(make_event(1000)).expect("oversized event should trigger flush");
        assert_eq!(batch.len(), 1);
        assert_eq!(buf.occupancy(), 0.0);
        assert!(buf.is_empty());
    }

    #[test]
    fn attributes_contribute_to_byte_count() {
        // Bare event (no attrs): 8+5+1+1+4+8 = 27 bytes.
        // With {"key":"value", "count":42}: +8 (3+5) + 13 (5+8) = 48 bytes.
        // Threshold of 40 sits between them — bare fits, attributed flushes.
        let make = |attrs: HashMap<String, AttributeValue>| LogEvent {
            timestamp: 0,
            level: Level::Debug,
            service: "x".to_string(),
            message: "y".to_string(),
            kafka_partition: 1,
            kafka_offset: 2,
            attributes: attrs,
        };

        let mut bare_buf = BatchBuffer::new(40);
        assert!(bare_buf.push(make(HashMap::new())).is_none(), "bare event should not flush");

        let mut attrs = HashMap::new();
        attrs.insert("key".to_string(), AttributeValue::String("value".to_string()));
        attrs.insert("count".to_string(), AttributeValue::Int(42));
        let mut attr_buf = BatchBuffer::new(40);
        assert!(attr_buf.push(make(attrs)).is_some(), "attributed event should flush");
    }
}
