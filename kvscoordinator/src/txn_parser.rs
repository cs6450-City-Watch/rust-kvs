//! Transaction parser module
//!
//! This module provides functionality to parse transaction specifications from text files.
//! It supports a simple DSL for defining transactions with begin/end blocks containing
//! get and put operations.

use regex::Regex;
use std::{fs, str::FromStr};

use crate::KvsOperation;

/// Parses transactions written to `file_path`
/// and returns them as a list of lists of operations (transactions).
///
/// ### Example file:
/// ```
/// begin
/// put(hello, 42)
/// get(hello)
/// end
/// ```
/// `end` used instead of something like `commit` or `abort`
/// as that decision depends on context beyond this file.
///
/// Whitespace is ignored, but each command should follow one of the following patterns:
/// - `begin` (just "begin")
/// - `put(<ID>, <VAL>)` (put followed by lparen followed by ID followed by comma followed by val followed by rparen)
/// - `get(<ID>)` (get followed by lparen followed by ID followed by rparen)
/// - `end`
///
/// Keywords `begin`, `put,` `get`, and `end` are parsed differently from identifiers
/// and should not be used as identifiers.
pub fn parse_transactions(file_path: String) -> Vec<Vec<KvsOperation>> {
    let source = fs::read_to_string(file_path).expect("couldn't read file");
    let mut tokens = tokenize(source).into_iter();

    let mut transactions = Vec::with_capacity(tokens.len());

    while let Some(transaction) = parse_single_transaction(&mut tokens) {
        transactions.push(transaction);
    }

    transactions
}

/// Parses a single transaction starting with `begin` and ending with `end`.
/// Returns None when there are no more transactions in the token stream.
fn parse_single_transaction(tokens: &mut impl Iterator<Item = Token>) -> Option<Vec<KvsOperation>> {
    let mut transaction = Vec::new();
    match tokens.next() {
        None => return None,
        Some(Token::Begin) => (),
        Some(_) => panic!("invalid transaction start"),
    }
    loop {
        let gotten_token = expect_token(tokens);
        match gotten_token {
            Token::End => break,
            Token::Put => {
                expect_variant(tokens, Token::LParen);
                let id = expect_identifier(tokens);
                expect_variant(tokens, Token::Comma);
                let val = expect_val(tokens);
                expect_variant(tokens, Token::RParen);
                transaction.push(KvsOperation::Put(id, val));
            }
            Token::Get => {
                expect_variant(tokens, Token::LParen);
                let id = expect_identifier(tokens);
                expect_variant(tokens, Token::RParen);
                transaction.push(KvsOperation::Get(id));
            }
            _ => panic!("expected an operation token, got {gotten_token:?}"),
        }
    }

    (!transaction.is_empty()).then_some(transaction)
}

/// Consumes the next token and panics if it doesn't match the expected variant.
fn expect_variant(tokens: &mut impl Iterator<Item = Token>, expected: Token) {
    let token = expect_token(tokens);

    assert!(
        token == expected,
        "expected token: {expected:?}, got token: {token:?}"
    );
}

/// Consumes the next token and returns its string value, panicking if it's not a string token.
fn expect_identifier(tokens: &mut impl Iterator<Item = Token>) -> String {
    let gotten_token = expect_token(tokens);
    match gotten_token {
        Token::Str(s) => s,
        _ => panic!(
            "expected token: {:?}, got token: {:?}",
            Token::Str("special string token".into()),
            gotten_token
        ),
    }
}

/// Consumes the next token and returns its numeric value, panicking if it's not a constant token.
fn expect_val(tokens: &mut impl Iterator<Item = Token>) -> u64 {
    let gotten_token = expect_token(tokens);
    match gotten_token {
        Token::Const(c) => c,
        _ => panic!(
            "expected token: {:?}, got token: {:?}",
            Token::Const(42),
            gotten_token
        ),
    }
}

/// Consumes the next token from the iterator, panicking if the stream is exhausted.
fn expect_token(tokens: &mut impl Iterator<Item = Token>) -> Token {
    match tokens.next() {
        Some(token) => token,
        None => panic!("severed stream"),
    }
}

use lazy_static::lazy_static;
lazy_static! {
    static ref idre: Regex =
        Regex::new(r"^[a-zA-Z_]\w*\b").expect("failure creating regex for keywords + identifiers");
    static ref constre: Regex = Regex::new(r"^[0-9]+\b").expect("failure creating constant regex");
    static ref single_char_re: Regex =
        Regex::new(r"^(\(|\)|,)").expect("failure creating regex for single char tokens");
}

/// Represents the different types of tokens in the transaction specification language.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Begin,
    Put,
    Get,
    End,
    LParen,
    RParen,
    Comma,
    Str(String), // Key
    Const(u64),  // Value
}

use thiserror::Error;

/// Errors that can occur during lexical analysis of transaction specifications.
#[derive(Error, Debug)]
pub enum LexError {
    #[error("Unrecognized token (strang {strang:?})")]
    Unrecognized { strang: String },
}

impl FromStr for Token {
    type Err = LexError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            r"(" => Ok(Self::LParen),
            r")" => Ok(Self::RParen),
            r"," => Ok(Self::Comma),
            r"begin" => Ok(Self::Begin),
            r"put" => Ok(Self::Put),
            r"get" => Ok(Self::Get),
            r"end" => Ok(Self::End),
            _ => Err(LexError::Unrecognized {
                strang: s.to_string(),
            }),
        }
    }
}

/// Converts the input source string into a sequence of tokens.
/// Uses regular expressions to identify keywords, identifiers, constants, and punctuation.
fn tokenize(source: String) -> Vec<Token> {
    let mut strang = source.as_str().trim();
    let mut tokens = Vec::new();
    while !(strang.is_empty()) {
        strang = strang.trim_start();
        tokens.push(if let Some(mat) = idre.find(strang) {
            strang = strang.trim_start_matches(mat.as_str());
            match Token::from_str(mat.as_str()) {
                Ok(token) => token,
                Err(_) => Token::Str(mat.as_str().into()),
            }
        } else if let Some(mat) = constre.find(strang) {
            strang = strang.trim_start_matches(mat.as_str());
            Token::Const(mat.as_str().parse().unwrap())
        } else if let Some(mat) = single_char_re.find(strang) {
            strang = strang.trim_start_matches(mat.as_str());
            Token::from_str(mat.as_str()).unwrap()
        } else {
            panic!("unrecognized: {strang}");
        });
    }

    tokens
}
