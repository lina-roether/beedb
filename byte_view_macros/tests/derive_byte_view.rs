use byte_view::{ByteView, ViewBuf};

#[derive(ByteView)]
struct TestStruct {
	value: u32,
	value2: u16,
}

#[test]
fn use_derived_sized() {
	let mut buf: ViewBuf<TestStruct> = ViewBuf::new();
	*buf = TestStruct {
		value: 2,
		value2: 1,
	};
	assert_eq!(buf.value, 2);
	assert_eq!(buf.value2, 1);
}

#[derive(ByteView)]
#[dynamically_sized]
struct TestStructUnsized {
	value: u32,
	value2: u16,
	items: [u64],
}

#[test]
fn use_derived_unsized() {
	let mut buf: ViewBuf<TestStructUnsized> = ViewBuf::new_with_size(24).unwrap();
	assert_eq!(buf.items.len(), 2);
	buf.value = 2;
	buf.value2 = 1;
	buf.items[0] = 25;
	buf.items[1] = 69;
	assert_eq!(buf.value, 2);
	assert_eq!(buf.value2, 1);
	assert_eq!(buf.items[0], 25);
	assert_eq!(buf.items[1], 69);
}
