// SPDX-License-Identifier: Apache-2.0

//! What the document's bytes are in the encoding they are parsed as.
//!
//! lol-html decodes text and attribute values with replacement: a byte
//! sequence that is not valid in the encoding in use becomes U+FFFD and
//! nothing says so. A windows-1252 page sent with no encoding is tokenized as
//! UTF-8, and every non-ASCII character in what the rules capture arrives as
//! the replacement character under a `started` that says UTF-8 and a
//! `finished` that says success.
//!
//! [`Tracker`] runs a second, non-replacing decoder over the same bytes and
//! counts what does not decode, so the trailer can report it. It follows the
//! one thing that can change the encoding mid-document, a `<meta charset>`
//! honoured under `adjust_charset_on_meta_tag`, using the same rule lol-html
//! applies and the tag's own byte offset.

use std::sync::{Arc, Mutex};

use encoding_rs::{Decoder, DecoderResult, Encoding};
use lol_html::AsciiCompatibleEncoding;

/// Scratch space the decoder writes into. The decoded text is discarded; only
/// the decoder's verdict on the input is kept.
const SCRATCH_BYTES: usize = 4096;

/// A charset switch a `<meta>` tag asked for: the byte offset just past the
/// tag, from which the new encoding applies, and the encoding.
pub type Switch = Arc<Mutex<Option<(usize, &'static Encoding)>>>;

/// The encoding a `<meta>` element declares, by the rule lol-html itself
/// uses for `adjust_charset_on_meta_tag`: a `charset` attribute first, and
/// otherwise the charset parameter of `content` when `http-equiv` is
/// `Content-Type`. Labels that are unknown, or that name an encoding lol-html
/// cannot tokenize, declare nothing.
#[must_use]
pub fn declared_by_meta(
    charset: Option<&str>,
    http_equiv: Option<&str>,
    content: Option<&str>,
) -> Option<&'static Encoding> {
    let ascii_compatible = |label: &str| {
        let encoding = Encoding::for_label_no_replacement(label.as_bytes())?;
        AsciiCompatibleEncoding::new(encoding).map(|_| encoding)
    };
    charset.and_then(ascii_compatible).or_else(|| {
        http_equiv.filter(|value| value.eq_ignore_ascii_case("Content-Type"))?;
        let mime = content?.parse::<mime::Mime>().ok()?;
        ascii_compatible(mime.get_param("charset")?.as_str())
    })
}

/// Counts the byte sequences of a document that do not decode.
pub struct Tracker {
    /// The encoding in use for the bytes still to come.
    encoding: &'static Encoding,
    /// A non-replacing decoder for it, carrying a sequence split by a chunk
    /// boundary from one chunk to the next.
    decoder: Decoder,
    /// Malformed sequences seen so far.
    malformed: u64,
    /// Document bytes fed so far, which is the offset of the next one.
    offset: usize,
    /// Where a `<meta>` handler leaves a switch for the tracker to apply.
    switch: Switch,
    /// Discarded decoder output.
    scratch: Box<[u8; SCRATCH_BYTES]>,
}

impl Tracker {
    /// Track a document that starts out in `encoding`.
    #[must_use]
    pub fn new(encoding: &'static Encoding) -> Self {
        Self {
            encoding,
            // lol-html decodes without BOM handling, so this does too: the
            // count has to be of what lol-html itself would replace.
            decoder: encoding.new_decoder_without_bom_handling(),
            malformed: 0,
            offset: 0,
            switch: Switch::default(),
            scratch: Box::new([0; SCRATCH_BYTES]),
        }
    }

    /// The slot a `<meta>` handler writes a charset switch into.
    #[must_use]
    pub fn switch(&self) -> Switch {
        Arc::clone(&self.switch)
    }

    /// The canonical label of the encoding in use for the bytes still to come.
    #[must_use]
    pub fn encoding_name(&self) -> &'static str {
        self.encoding.name()
    }

    /// Account for the next chunk of the document, after the rewriter has
    /// been given it, so that a switch its handlers recorded is applied at
    /// the offset it belongs to.
    pub fn feed(&mut self, chunk: &[u8]) {
        let switch = self.switch.lock().expect("charset switch lock").take();
        let Some((at, encoding)) = switch else {
            self.decode(chunk, false);
            self.offset += chunk.len();
            return;
        };
        // The tag that asked for the switch ended inside this chunk: its last
        // byte is what made the rewriter produce it.
        let split = at.saturating_sub(self.offset).min(chunk.len());
        let (before, after) = chunk.split_at(split);
        // A tag ends on `>`, so no sequence is left open across the switch,
        // and the old decoder is finished to say so.
        self.decode(before, true);
        self.encoding = encoding;
        self.decoder = encoding.new_decoder_without_bom_handling();
        self.decode(after, false);
        self.offset += chunk.len();
    }

    /// The count once the document is over. A sequence the input ends in the
    /// middle of is malformed too.
    pub fn finish(&mut self) -> u64 {
        self.decode(&[], true);
        self.malformed
    }

    /// The count so far, for a run that stops before the end of the input.
    #[must_use]
    pub const fn malformed(&self) -> u64 {
        self.malformed
    }

    /// Run `bytes` through the decoder, counting what it refuses.
    fn decode(&mut self, mut bytes: &[u8], last: bool) {
        loop {
            let (result, read, _) = self.decoder.decode_to_utf8_without_replacement(
                bytes,
                self.scratch.as_mut_slice(),
                last,
            );
            bytes = &bytes[read..];
            match result {
                DecoderResult::InputEmpty => return,
                DecoderResult::OutputFull => {}
                DecoderResult::Malformed(_, _) => self.malformed += 1,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Tracker, declared_by_meta};

    #[test]
    fn a_sequence_split_across_chunks_is_not_malformed() {
        // "é" is c3 a9 in UTF-8; the chunk boundary falls between the bytes.
        let mut tracker = Tracker::new(encoding_rs::UTF_8);
        tracker.feed(b"caf\xc3");
        tracker.feed(b"\xa9 au lait");
        assert_eq!(tracker.finish(), 0);
    }

    #[test]
    fn each_undecodable_sequence_is_counted_once() {
        // windows-1252 "é", "ü", "ß" read as UTF-8: three lone bytes.
        let mut tracker = Tracker::new(encoding_rs::UTF_8);
        tracker.feed(b"caf\xe9 f\xfcr Stra\xdfe");
        assert_eq!(tracker.finish(), 3);
        // The same bytes are sound in the encoding they were written in.
        let mut tracker = Tracker::new(encoding_rs::WINDOWS_1252);
        tracker.feed(b"caf\xe9 f\xfcr Stra\xdfe");
        assert_eq!(tracker.finish(), 0);
    }

    #[test]
    fn a_sequence_the_input_ends_inside_is_malformed() {
        let mut tracker = Tracker::new(encoding_rs::UTF_8);
        tracker.feed(b"caf\xc3");
        assert_eq!(tracker.malformed(), 0, "it may still be completed");
        assert_eq!(tracker.finish(), 1);
    }

    #[test]
    fn a_switch_applies_from_its_offset_and_not_before() {
        let mut tracker = Tracker::new(encoding_rs::UTF_8);
        // One byte that is not UTF-8, then a tag ending at offset 9, then
        // windows-1251 text that is sound once the switch is honoured.
        let chunk = b"\xff<meta x>\xcf\xf0\xe8\xe2\xe5\xf2";
        *tracker.switch().lock().unwrap() = Some((9, encoding_rs::WINDOWS_1251));
        tracker.feed(chunk);
        assert_eq!(tracker.encoding_name(), "windows-1251");
        assert_eq!(tracker.finish(), 1, "only the byte before the tag");
    }

    #[test]
    fn meta_declarations_follow_lol_html() {
        let name = |charset, http_equiv, content| {
            declared_by_meta(charset, http_equiv, content).map(encoding_rs::Encoding::name)
        };
        assert_eq!(name(Some("windows-1251"), None, None), Some("windows-1251"));
        assert_eq!(
            name(
                None,
                Some("content-type"),
                Some("text/html; charset=Shift_JIS")
            ),
            Some("Shift_JIS")
        );
        // Not a content type declaration, an unknown label, and an encoding
        // lol-html cannot tokenize: none of them switches anything.
        assert_eq!(
            name(None, Some("refresh"), Some("text/html; charset=utf-8")),
            None
        );
        assert_eq!(name(Some("no-such-charset"), None, None), None);
        assert_eq!(name(Some("utf-16le"), None, None), None);
    }
}
