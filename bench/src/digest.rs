// SPDX-License-Identifier: Apache-2.0

//! An order-sensitive digest of matched elements, so the harness can prove
//! the arms did the same work before it reports how fast they did it.
//!
//! FNV-1a rather than anything cryptographic: this is guarding against a
//! benchmark that quietly measures two different workloads, not against an
//! adversary. It is stable across runs and across machines, which matters
//! because the digests are pasted into RESULTS.md.

use std::fmt;

use grpc_lol_html::proto::v1 as pb;

const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, PartialEq, Eq)]
pub struct Digest {
    hash: u64,
    elements: u64,
    attributes: u64,
}

impl Digest {
    pub const fn new() -> Self {
        Self {
            hash: OFFSET,
            elements: 0,
            attributes: 0,
        }
    }

    fn eat(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash ^= u64::from(*byte);
            self.hash = self.hash.wrapping_mul(PRIME);
        }
        // A separator, so ("ab", "c") and ("a", "bc") do not collide.
        self.hash ^= 0xff;
        self.hash = self.hash.wrapping_mul(PRIME);
    }

    fn start(&mut self, rule: &str, tag: &str) {
        self.elements += 1;
        self.eat(rule.as_bytes());
        self.eat(tag.as_bytes());
    }

    fn attribute(&mut self, name: &str, value: &str) {
        self.attributes += 1;
        self.eat(name.as_bytes());
        self.eat(value.as_bytes());
    }

    /// Fold in one match from the in-process lol-html arm.
    pub fn element(
        &mut self,
        rule: &str,
        tag: &str,
        attributes: &[lol_html::html_content::Attribute<'_>],
    ) {
        self.start(rule, tag);
        for attribute in attributes {
            self.attribute(&attribute.name(), &attribute.value());
        }
    }

    /// Fold in one match from the gRPC arm.
    pub fn element_pb(&mut self, rule: &str, tag: &str, attributes: &[pb::Attribute]) {
        self.start(rule, tag);
        for attribute in attributes {
            self.attribute(&attribute.name, &attribute.value);
        }
    }

    /// Fold in one match from the DOM arm.
    pub fn element_dom<'a>(
        &mut self,
        rule: &str,
        tag: &str,
        attributes: impl Iterator<Item = (&'a str, &'a str)>,
    ) {
        self.start(rule, tag);
        for (name, value) in attributes {
            self.attribute(name, value);
        }
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:016x}/{}el/{}attr",
            self.hash, self.elements, self.attributes
        )
    }
}
