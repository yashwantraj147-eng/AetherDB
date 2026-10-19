package com.aetherdb.storage.columnar;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * High-performance decoder for compressed columnar blocks.
 */
public class ColumnDecoder {

    private final ColumnType type;

    public ColumnDecoder(ColumnType type) {
        this.type = type;
    }

    /**
     * Decodes compressed column bytes into a list of typed ColumnValues.
     */
    public List<ColumnValue> decode(byte[] data) {
        if (data == null || data.length < 5) {
            return new ArrayList<>();
        }

        ByteBuffer buf = ByteBuffer.wrap(data);
        byte codecId = buf.get();
        int rowCount = buf.getInt();
        ColumnEncoder.Codec codec = ColumnEncoder.Codec.fromId(codecId);

        return switch (codec) {
            case RAW -> decodeRaw(buf, rowCount);
            case DICTIONARY -> decodeDictionary(buf, rowCount);
            case RLE -> decodeRLE(buf, rowCount);
        };
    }

    private List<ColumnValue> decodeRaw(ByteBuffer buf, int rowCount) {
        List<ColumnValue> list = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            byte notNull = buf.get();
            if (notNull == 0) {
                list.add(ColumnValue.ofNull(type));
            } else {
                list.add(readValue(buf));
            }
        }
        return list;
    }

    private List<ColumnValue> decodeDictionary(ByteBuffer buf, int rowCount) {
        int dictSize = BinaryUtils.readVarInt(buf);
        List<ColumnValue> dictionary = new ArrayList<>(dictSize);
        for (int i = 0; i < dictSize; i++) {
            byte notNull = buf.get();
            if (notNull == 0) {
                dictionary.add(ColumnValue.ofNull(type));
            } else {
                dictionary.add(readValue(buf));
            }
        }

        List<ColumnValue> list = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            int dictIdx = BinaryUtils.readVarInt(buf);
            list.add(dictionary.get(dictIdx));
        }
        return list;
    }

    private List<ColumnValue> decodeRLE(ByteBuffer buf, int rowCount) {
        int runCount = BinaryUtils.readVarInt(buf);
        List<ColumnValue> list = new ArrayList<>(rowCount);

        for (int i = 0; i < runCount; i++) {
            int runLen = BinaryUtils.readVarInt(buf);
            byte notNull = buf.get();
            ColumnValue val = (notNull == 0) ? ColumnValue.ofNull(type) : readValue(buf);
            for (int r = 0; r < runLen; r++) {
                list.add(val);
            }
        }
        return list;
    }

    private ColumnValue readValue(ByteBuffer buf) {
        return switch (type) {
            case INT32 -> ColumnValue.ofInt(buf.getInt());
            case INT64 -> ColumnValue.ofLong(buf.getLong());
            case TIMESTAMP -> ColumnValue.ofTimestamp(buf.getLong());
            case FLOAT64 -> ColumnValue.ofDouble(buf.getDouble());
            case STRING -> ColumnValue.ofString(BinaryUtils.readString(buf));
            case BOOLEAN -> ColumnValue.ofBoolean(buf.get() == 1);
        };
    }
}
