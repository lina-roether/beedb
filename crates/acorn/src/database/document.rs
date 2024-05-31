use std::{
	collections::HashMap,
	error::Error,
	fmt::{self},
	io::Write,
};

#[derive(Debug, Clone)]
enum SchemaAccessStep {
	Index(usize),
	Entry(String),
}

impl fmt::Display for SchemaAccessStep {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Index(index) => write!(f, "[{index}]"),
			Self::Entry(key) => write!(f, ".{key}"),
		}
	}
}

#[derive(Debug)]
pub(crate) struct SchemaError {
	access_stack: Vec<SchemaAccessStep>,
	expected: Schema,
	received: Primitive,
}

impl SchemaError {
	fn new(expected: Schema, received: Primitive) -> Self {
		Self {
			expected,
			received,
			access_stack: Vec::new(),
		}
	}

	fn push_access_step(&mut self, step: SchemaAccessStep) {
		self.access_stack.push(step)
	}
}

impl fmt::Display for SchemaError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Schema expected {} at ", self.expected)?;
		for step in self.access_stack.iter().rev() {
			write!(f, "{step}")?;
		}
		write!(f, ", but found {}", self.received)?;
		Ok(())
	}
}

impl Error for SchemaError {}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Value {
	String(String),
	Bool(bool),
	Int(i64),
	Uint(u64),
	Float(f64),
}

impl Value {
	pub fn primitive(&self) -> Primitive {
		match self {
			Self::String(..) => Primitive::String,
			Self::Bool(..) => Primitive::Bool,
			Self::Int(..) => Primitive::Int,
			Self::Uint(..) => Primitive::Uint,
			Self::Float(..) => Primitive::Float,
		}
	}
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) enum HashableValue {
	String(String),
	Bool(bool),
	Int(i64),
	Uint(u64),
}

impl From<HashableValue> for Value {
	fn from(value: HashableValue) -> Self {
		match value {
			HashableValue::String(string) => Self::String(string),
			HashableValue::Bool(bool) => Self::Bool(bool),
			HashableValue::Int(int) => Self::Int(int),
			HashableValue::Uint(uint) => Self::Uint(uint),
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Document {
	Nil,
	Value(Value),
	List(Vec<Document>),
	Map(HashMap<HashableValue, Document>),
	Struct(HashMap<String, Document>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Primitive {
	String,
	Bool,
	Int,
	Uint,
	Float,
}

impl fmt::Display for Primitive {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::String => write!(f, "string"),
			Self::Bool => write!(f, "bool"),
			Self::Int => write!(f, "int"),
			Self::Uint => write!(f, "uint"),
			Self::Float => write!(f, "float"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashablePrimitive {
	String,
	Bool,
	Int,
	Uint,
}

impl From<HashablePrimitive> for Primitive {
	fn from(value: HashablePrimitive) -> Self {
		match value {
			HashablePrimitive::String => Self::String,
			HashablePrimitive::Bool => Self::Bool,
			HashablePrimitive::Int => Self::Int,
			HashablePrimitive::Uint => Self::Uint,
		}
	}
}

impl fmt::Display for HashablePrimitive {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Primitive::from(*self).fmt(f)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Schema {
	Empty,
	Primitive(Primitive),
	Option(Box<Schema>),
	List(Box<Schema>),
	Map(HashablePrimitive, Box<Schema>),
	Struct(HashMap<String, Box<Schema>>),
	Enum(HashMap<String, Box<Schema>>),
}

impl fmt::Display for Schema {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Empty => write!(f, "()"),
			Self::Primitive(primitive) => write!(f, "{primitive}"),
			Self::Option(inner) => write!(f, "?{inner}"),
			Self::List(items) => write!(f, "[{items}]"),
			Self::Map(key, value) => write!(f, "[{key} => {value}]"),
			Self::Struct(entries) => {
				write!(f, "{{ ")?;

				for (i, (key, schema)) in entries.iter().enumerate() {
					write!(f, "{key}: {schema}")?;
					if i < entries.len() - 1 {
						write!(f, ", ")?;
					}
				}
				write!(f, " }}")?;
				Ok(())
			}
			Self::Enum(variants) => {
				for (i, (name, schema)) in variants.iter().enumerate() {
					write!(f, "{name}({schema})")?;
					if i < variants.len() - 1 {
						write!(f, " | ")?;
					}
				}
				Ok(())
			}
		}
	}
}
