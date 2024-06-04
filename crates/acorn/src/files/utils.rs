use crc::Crc;

// TODO: there are tradeoffs here. Perhaps I should look more into selecting an
// algorithm.
pub(crate) const CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
pub(crate) const CRC16: Crc<u16> = Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
