package sstable

import "encoding/binary"

type bloomFilter struct {
	bits []byte
	k    uint8
}

func buildBloom(keys []string, bitsPerKey int) []byte {
	bitCount := len(keys) * bitsPerKey
	if bitCount < 64 {
		bitCount = 64
	}
	byteCount := (bitCount + 7) / 8
	bitCount = byteCount * 8
	k := bitsPerKey * 69 / 100
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	data := make([]byte, 5+byteCount)
	binary.LittleEndian.PutUint32(data[:4], uint32(bitCount))
	data[4] = byte(k)
	bits := data[5:]
	for _, key := range keys {
		hash := bloomHash([]byte(key))
		delta := hash>>17 | hash<<15
		for i := 0; i < k; i++ {
			position := hash % uint32(bitCount)
			bits[position/8] |= 1 << (position % 8)
			hash += delta
		}
	}
	return data
}

func decodeBloom(data []byte) (bloomFilter, bool) {
	if len(data) < 5 {
		return bloomFilter{}, false
	}
	bitCount := binary.LittleEndian.Uint32(data[:4])
	if bitCount == 0 || int((bitCount+7)/8) != len(data)-5 || data[4] == 0 {
		return bloomFilter{}, false
	}
	return bloomFilter{bits: append([]byte(nil), data[5:]...), k: data[4]}, true
}

func (f bloomFilter) mayContain(key string) bool {
	if len(f.bits) == 0 {
		return true
	}
	bitCount := uint32(len(f.bits) * 8)
	hash := bloomHash([]byte(key))
	delta := hash>>17 | hash<<15
	for i := uint8(0); i < f.k; i++ {
		position := hash % bitCount
		if f.bits[position/8]&(1<<(position%8)) == 0 {
			return false
		}
		hash += delta
	}
	return true
}

// 与 LevelDB BloomPolicy 相同的稳定 32 位散列，避免 Go map hash 的随机种子。
func bloomHash(data []byte) uint32 {
	const seed uint32 = 0xbc9f1d34
	h := seed ^ uint32(len(data))*0xc6a4a793
	for len(data) >= 4 {
		word := binary.LittleEndian.Uint32(data[:4])
		h += word
		h *= 0xc6a4a793
		h ^= h >> 16
		data = data[4:]
	}
	switch len(data) {
	case 3:
		h += uint32(data[2]) << 16
		fallthrough
	case 2:
		h += uint32(data[1]) << 8
		fallthrough
	case 1:
		h += uint32(data[0])
		h *= 0xc6a4a793
		h ^= h >> 24
	}
	return h
}
