use squirrel::common::slot_meta::SlotMeta;
use std::fs::File;
use std::io::Read;

#[test]
fn test_load_slot_meta_msgpack() {
    // 指定msgpack文件路径
    let path = "/home/gold_dog/Cank/Archieve/362690000_362694999.meta";
    let mut file = File::open(path).expect("cannot open slot_meta msgpack file");
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).expect("cannot read file");
    // 反序列化msgpack为 Vec<SlotMeta>
    let slots: Vec<SlotMeta> = rmp_serde::from_slice(&buf).expect("msgpack parse error");
    println!("slot_meta count: {}", slots.len());
    if let Some(first) = slots.first() {
        println!("first slot_meta: {:?}", first);
    }
    // 这里只做能否load的测试
}
