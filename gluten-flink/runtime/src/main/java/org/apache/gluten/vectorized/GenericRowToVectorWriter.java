/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gluten.vectorized;

import io.github.zhztheplayer.velox4j.type.*;

import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.*;
import org.apache.flink.table.data.RowData;
 
import java.util.ArrayList;
import java.util.List;

public abstract class GenericRowToVectorWriter {
    public static GenericRowToVectorWriter create(
        String fieldName, Type fieldType, BufferAllocator allocator) {
        return create(fieldName, fieldType, allocator, null);
    }
    public static GenericRowToVectorWriter create(
        String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
        if (vector == null) {
            vector = FieldVectorCreator.create(
                fieldName, fieldType, false, allocator, null);
        }
        if (fieldType instanceof IntegerType) {
            return new GenericRowToIntVectorWriter(fieldName, fieldType, allocator, vector);
        } else if (fieldType instanceof BooleanType) {
            return new GenericRowToBooleanVectorWriter(fieldName, fieldType, allocator, vector);
        } else if (fieldType instanceof BigIntType) {
            return new GenericRowToBigIntVectorWriter(fieldName, fieldType, allocator, vector);
        } else if (fieldType instanceof VarCharType) {
            return new GenericRowToVarCharVectorWriter(fieldName, fieldType, allocator, vector);
        } else if (fieldType instanceof TimestampType) {
            return new GenericRowToTimestampVectorWriter(fieldName, fieldType, allocator, vector);
        } else if (fieldType instanceof RowType) {
            return new GenericRowToStructVectorWriter(fieldName, fieldType, allocator, vector);
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }
    public void add(int fieldIndex, RowData rowData) {
        throw new UnsupportedOperationException("assign is not supported");
    }

    FieldVector getVector() {
        throw new UnsupportedOperationException("getVector is not supported");
    }
}
// Build FieldVector from Type.
class FieldVectorCreator {
    public static FieldVector create(
        String name, Type dataType, boolean nullable, BufferAllocator allocator, String timeZoneId) {
        Field field = toArrowField(name, dataType, nullable, timeZoneId);
        return field.createVector(allocator);
    }

    private static ArrowType toArroyType(Type dataType, String timeZoneId) {
        if (dataType instanceof BooleanType) {
            return ArrowType.Bool.INSTANCE;
        } else if (dataType instanceof IntegerType) {
            return new ArrowType.Int(8 * 4, true);
        } else if (dataType instanceof BigIntType) {
            return new ArrowType.Int(8 * 8, true);
        } else if (dataType instanceof VarCharType) {
            return ArrowType.Utf8.INSTANCE;
        } else if (dataType instanceof TimestampType) {
            if (timeZoneId == null) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
            } else {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, timeZoneId);
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static Field toArrowField(
        String name, Type dataType, boolean nullable, String timeZoneId) {
        if (dataType instanceof ArrayType) {
            throw new UnsupportedOperationException("ArrayType is not supported");
        } else if (dataType instanceof MapType) {
            throw new UnsupportedOperationException("MapType is not supported");
        } else if (dataType instanceof RowType) {
            RowType structType = (RowType) dataType;
            List<String> fieldNames = structType.getNames();
            List<Type> fieldTypes = structType.getChildren();
            List<Field> subFields = new ArrayList<>();
            for (int i = 0; i < structType.getChildren().size(); ++i) {
                subFields.add(
                    toArrowField(fieldNames.get(i), fieldTypes.get(i), nullable, timeZoneId));
            }
            FieldType strcutType =
                new FieldType(nullable, ArrowType.Struct.INSTANCE, null);
            return new Field(name, strcutType, subFields);
        } else {
            // TODO: support nullable
            ArrowType arrowType = toArroyType(dataType, timeZoneId);
            FieldType fieldType = new FieldType(nullable, arrowType, null);
            return new Field(name, fieldType, new ArrayList<>());
        }
    }
}

class GenericRowToIntVectorWriter extends GenericRowToVectorWriter {
    private final IntVector vector;
    private int valueCount = 0;;

    public GenericRowToIntVectorWriter(
        String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
        this.vector = (IntVector) vector;
    }

    @Override
    public void add(int fieldIndex, RowData rowData) {
        vector.setSafe(valueCount, rowData.getInt(fieldIndex));
        valueCount++;
        vector.setValueCount(valueCount);
    }

    @Override
    public FieldVector getVector() {
        return vector;
    }
}

class GenericRowToBooleanVectorWriter extends GenericRowToVectorWriter {
    private final BitVector vector;
    private int valueCount = 0;;

    public GenericRowToBooleanVectorWriter(
        String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
        // this.vector = new BitVector(fieldName, allocator);
        this.vector = (BitVector) vector;
    }

    @Override
    public void add(int fieldIndex, RowData rowData) {
        vector.setSafe(valueCount, rowData.getBoolean(fieldIndex) ? 1 : 0);
        valueCount++;
        vector.setValueCount(valueCount);
    }
    @Override
    public FieldVector getVector() {
        return vector;
    }
}


class GenericRowToBigIntVectorWriter extends GenericRowToVectorWriter {
    private final BigIntVector vector;
    private int valueCount = 0;;

    public GenericRowToBigIntVectorWriter(
        String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
        // this.vector = new BigIntVector(fieldName, allocator);
        this.vector = (BigIntVector) vector;
    }

    @Override
    public void add(int fieldIndex, RowData rowData) {
        vector.setSafe(valueCount, rowData.getLong(fieldIndex));
        valueCount++;
        vector.setValueCount(valueCount);
    }
    @Override
    public FieldVector getVector() {
        return vector;
    }
}

class GenericRowToVarCharVectorWriter extends GenericRowToVectorWriter {
    private final VarCharVector vector;
    private int valueCount = 0;;

    public GenericRowToVarCharVectorWriter(
        String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
        // this.vector = new VarCharVector(fieldName, allocator);
        this.vector = (VarCharVector) vector;
    }

    @Override
    public void add(int fieldIndex, RowData rowData) {
        vector.setSafe(valueCount, rowData.getString(fieldIndex).toBytes());
        valueCount++;
        vector.setValueCount(valueCount);

    }
    @Override
    public FieldVector getVector() {
        return vector;
    }
}

class GenericRowToTimestampVectorWriter extends GenericRowToVectorWriter {
    private final TimeStampMilliVector vector;
    private int valueCount = 0;;

    public GenericRowToTimestampVectorWriter(
        String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
        // this.vector = new TimeStampMilliVector(fieldName, allocator);
        this.vector = (TimeStampMilliVector) vector;
    }

    @Override
    public void add(int fieldIndex, RowData rowData) {
        // TODO: support precision
        vector.setSafe(valueCount, rowData.getTimestamp(fieldIndex, 3).getMillisecond());
        valueCount++;
        vector.setValueCount(valueCount);
    }

    @Override
    public FieldVector getVector() {
        return vector;
    }
}

class GenericRowToStructVectorWriter extends GenericRowToVectorWriter {
    private final RowType rowType;
    private int fieldCounts = 0;
    BufferAllocator allocator;
    private List<GenericRowToVectorWriter> subFieldWriters;
    private final List<String> subFieldNames;
    private final String fieldName;
    private int valueCount = 0;
    private StructVector vector;

    public GenericRowToStructVectorWriter(
        String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
        this.fieldName = fieldName;
        this.vector = (StructVector) vector;
        this.rowType = (RowType) fieldType;
        subFieldNames = this.rowType.getNames();
        subFieldWriters = new ArrayList<>();
        for (int i = 0; i < subFieldNames.size(); ++i) {
            subFieldWriters.add(
                GenericRowToVectorWriter.create(
                    subFieldNames.get(i),
                    rowType.getChildren().get(i),
                    allocator,
                    (FieldVector)(this.vector.getChildByOrdinal(i))
                    ));
        }
        fieldCounts = subFieldNames.size();
    }

    @Override
    public void add(int fieldIndex, RowData rowData) {
        RowData subRowData = rowData.getRow(fieldIndex, fieldCounts);
        for (int i = 0; i < fieldCounts; i++) {
            subFieldWriters.get(i).add(i, subRowData);
        }
        valueCount++;
        vector.setValueCount(valueCount);
        BigIntVector offsetVector = (BigIntVector) vector.getChildByOrdinal(0);
    }

    @Override
    public FieldVector getVector() {
        return vector;
    }
}