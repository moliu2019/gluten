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

import org.apache.arrow.vector.*;
import org.apache.flink.table.data.binary.BinaryStringData;

import io.github.zhztheplayer.velox4j.type.*;

// This module is use to convert colunn vector to flink generic rows.
public abstract class VectorGenericRowAccessor {
    public static VectorGenericRowAccessor initAccessor(FieldVector vector) {
        if (vector instanceof BitVector) {
            return new BooleanVectorGenericRowAccessor(vector);
        } else if (vector instanceof IntVector) {
            return new IntVectorGenericRowAccessor(vector);
        } else if (vector instanceof BigIntVector) {
            return new BigIntVectorGenericRowAccessor(vector);
        } else if (vector instanceof Float8Vector) {
            return new DoubleVectorGenericRowAccessor(vector);
        } else if (vector instanceof VarCharVector) {
            return new VarCharVectorGenericRowAccessor(vector);
        } 
        else {
            throw new UnsupportedOperationException("Unsupported type: " + vector.getClass().getName());
        }
    }

    // A general method to extract values from the vector.
    public Object get(int index) {
        throw new UnsupportedOperationException("get not supported");
    }
}

class BooleanVectorGenericRowAccessor extends VectorGenericRowAccessor {
    private BitVector vector = null;
    public BooleanVectorGenericRowAccessor(FieldVector vector) {
        this.vector = (BitVector) vector;
    }
    @Override
    public Object get(int index) {
        return vector.get(index) != 0;
    }
}

class IntVectorGenericRowAccessor extends VectorGenericRowAccessor {
    private IntVector vector = null;
    public IntVectorGenericRowAccessor(FieldVector vector) {
        this.vector = (IntVector) vector;
    }
    @Override
    public Object get(int index) {
        return vector.get(index);
    }
}

class BigIntVectorGenericRowAccessor extends VectorGenericRowAccessor {
    private BigIntVector vector;
    public BigIntVectorGenericRowAccessor(FieldVector vector) {
        this.vector = (BigIntVector) vector;
    }
    @Override
    public Object get(int index) {
        return vector.get(index);
    }
}

class DoubleVectorGenericRowAccessor extends VectorGenericRowAccessor {
    private Float8Vector vector = null;
    public DoubleVectorGenericRowAccessor(FieldVector vector) {
        this.vector = (Float8Vector) vector;
    }
    @Override
    public Object get(int index) {
        return vector.get(index);
    }
}

class VarCharVectorGenericRowAccessor extends VectorGenericRowAccessor {
    private VarCharVector vector;
    public VarCharVectorGenericRowAccessor(FieldVector vector) {
        this.vector = (VarCharVector) vector;
    }
    @Override
    public Object get(int index) {
        return BinaryStringData.fromBytes(vector.get(index));
    }
}

