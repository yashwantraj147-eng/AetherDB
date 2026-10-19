package com.aetherdb.storage.columnar;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Adaptive Column Encoder that selects the optimal compression codec
 * (Dictionary, Run-Length Encoding, or Raw) based on cardinality and value repetition.
 */
public class ColumnEncoder {

    public enum Codec {
        RAW((byte) 0),
        DICTIONARY((byte) 1),
        RLE((byte) 2);

        private final byte id;
        Codec(byte id) { this.id = id; }
        public byte getId() { return id; }
        public static Codec fromId(byte id) {
            for (Codec c : values()) {
                if (c.id == id) return c;
            }
            return RAW;
        }
    }

    private final ColumnType type;
    private final double dictThresholdRatio;

    public ColumnEncoder(ColumnType type, double dictThresholdRatio) {
        this.type = type;
        this.dictThresholdRatio = dictThresholdRatio;
    }

    /**
     * Encodes a list of column values into a compressed binary representation.
     */
    public byte[] encode(List<ColumnValue> values) {
        if (values.isEmpty()) {
            return new byte[]{Codec.RAW.getId(), 0, 0, 0, 0};
        }

        // Test for RLE suitability: count consecutive runs
        int runCount = 1;
        for (int i = 1; i < values.size(); i++) {
            if (!Objects.equals(values.get(i), values.get(i - 1))) {
                runCount++;
            }
        }

        // If average run length is >= 3, use RLE
        if ((double) values.size() / runCount >= 3.0) {
            return encodeRLE(values);
        }

        // Test for Dictionary suitability: distinct count / total count
        Set<ColumnValue> distinctSet = new HashSet<>(values);
        double cardinalityRatio = (double) distinctSet.size() / values.size();

        if (cardinalityRatio <= dictThresholdRatio && distinctSet.size() <= 65535) {
            return encodeDictionary(values, distinctSet);
        }

        // Fallback to RAW encoding
        return encodeRaw(values);
    }

    private byte[] encodeRaw(List<ColumnValue> values) {
        ByteBuffer buf = ByteBuffer.allocate(values.size() * 16 + 64);
        buf.put(Codec.RAW.getId());
        buf.putInt(values.size());

        for (ColumnValue val : values) {
            if (val == null || val.isNull()) {
                buf.put((byte) 0);
            } else {
                buf.put((byte) 1);
                writeValue(buf, val);
            }
        }

        byte[] result = new byte[buf.position()];
        buf.flip();
        buf.get(result);
        return result;
    }

    private byte[] encodeDictionary(List<ColumnValue> values, Set<ColumnValue> distinctSet) {
        List<ColumnValue> dictionary = new ArrayList<>(distinctSet);
        Map<ColumnValue, Integer> dictMap = new HashMap<>();
        for (int i = 0; i < dictionary.size(); i++) {
            dictMap.put(dictionary.get(i), i);
        }

        ByteBuffer buf = ByteBuffer.allocate(values.size() * 4 + dictionary.size() * 16 + 64);
        buf.put(Codec.DICTIONARY.getId());
        buf.putInt(values.size());

        // Write Dictionary
        BinaryUtils.writeVarInt(buf, dictionary.size());
        for (ColumnValue dictVal : dictionary) {
            if (dictVal == null || dictVal.isNull()) {
                buf.put((byte) 0);
            } else {
                buf.put((byte) 1);
                writeValue(buf, dictVal);
            }
        }

        // Write Dictionary Indices
        for (ColumnValue val : values) {
            Integer idx = dictMap.get(val);
            BinaryUtils.writeVarInt(buf, idx != null ? idx : 0);
        }

        byte[] result = new byte[buf.position()];
        buf.flip();
        buf.get(result);
        return result;
    }

    private byte[] encodeRLE(List<ColumnValue> values) {
        ByteBuffer buf = ByteBuffer.allocate(values.size() * 12 + 64);
        buf.put(Codec.RLE.getId());
        buf.putInt(values.size());

        List<Integer> runLengths = new ArrayList<>();
        List<ColumnValue> runValues = new ArrayList<>();

        ColumnValue currentVal = values.get(0);
        int currentLen = 1;

        for (int i = 1; i < values.size(); i++) {
            ColumnValue v = values.get(i);
            if (Objects.equals(v, currentVal)) {
                currentLen++;
            } else {
                runLengths.add(currentLen);
                runValues.add(currentVal);
                currentVal = v;
                currentLen = 1;
            }
        }
        runLengths.add(currentLen);
        runValues.add(currentVal);

        BinaryUtils.writeVarInt(buf, runValues.size());
        for (int i = 0; i < runValues.size(); i++) {
            BinaryUtils.writeVarInt(buf, runLengths.get(i));
            ColumnValue val = runValues.get(i);
            if (val == null || val.isNull()) {
                buf.put((byte) 0);
            } else {
                buf.put((byte) 1);
                writeValue(buf, val);
            }
        }

        byte[] result = new byte[buf.position()];
        buf.flip();
        buf.get(result);
        return result;
    }

    private void writeValue(ByteBuffer buf, ColumnValue val) {
        switch (type) {
            case INT32 -> buf.putInt(val.asInt());
            case INT64, TIMESTAMP -> buf.putLong(val.asLong());
            case FLOAT64 -> buf.putDouble(val.asDouble());
            case STRING -> BinaryUtils.writeString(buf, val.asString());
            case BOOLEAN -> buf.put((byte) (val.asBoolean() ? 1 : 0));
        }
    }
}
