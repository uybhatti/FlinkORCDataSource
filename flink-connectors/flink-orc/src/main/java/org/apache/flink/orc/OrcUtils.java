/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import scala.tools.cmd.gen.AnyVals;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

public class OrcUtils {

	public static TypeInformation schemaToTypeInfo(TypeDescription schema) {
		switch (schema.getCategory()) {
			case BOOLEAN:
				return BasicTypeInfo.BOOLEAN_TYPE_INFO;
			case BYTE:
				return BasicTypeInfo.BYTE_TYPE_INFO;
			case SHORT:
				return BasicTypeInfo.SHORT_TYPE_INFO;
			case INT:
				return BasicTypeInfo.INT_TYPE_INFO;
			case LONG:
				return BasicTypeInfo.LONG_TYPE_INFO;
			case FLOAT:
				return BasicTypeInfo.FLOAT_TYPE_INFO;
			case DOUBLE:
				return BasicTypeInfo.DOUBLE_TYPE_INFO;
			case STRING:
			case CHAR:
			case VARCHAR:
				return BasicTypeInfo.STRING_TYPE_INFO;
			case DATE:
				return SqlTimeTypeInfo.DATE;
			case TIMESTAMP:
				return SqlTimeTypeInfo.TIMESTAMP;
			case BINARY:
				return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			case STRUCT:
				List<TypeDescription> fieldSchemas = schema.getChildren();
				TypeInformation[] fieldTypes = new TypeInformation[fieldSchemas.size()];
				for (int i = 0; i < fieldSchemas.size(); i++) {
					fieldTypes[i] = schemaToTypeInfo(fieldSchemas.get(i));
				}
				String[] fieldNames = schema.getFieldNames().toArray(new String[]{});
				return new RowTypeInfo(fieldTypes, fieldNames);
			case LIST:
				TypeDescription elementSchema = schema.getChildren().get(0);
				TypeInformation elementType = schemaToTypeInfo(elementSchema);
				ObjectArrayTypeInfo t = new ObjectArrayTypeInfo(Object.class,elementType);
				return t;
				//return new ListTypeInfo(elementType);
			case MAP:
				TypeDescription keySchema = schema.getChildren().get(0);
				TypeDescription valSchema = schema.getChildren().get(1);
				TypeInformation keyType = schemaToTypeInfo(keySchema);
				TypeInformation valType = schemaToTypeInfo(valSchema);
				return new MapTypeInfo(keyType, valType);
			case DECIMAL:
				return BasicTypeInfo.BIG_DEC_TYPE_INFO;
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet.");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	public static void fillRows(Row[] rows, TypeDescription schema, VectorizedRowBatch batch, int[] fieldMapping) {

		List<TypeDescription> fieldTypes = schema.getChildren();

		//System.out.println("batch: "+batch + "==");

		//System.out.println(fieldTypes);

		for (int outIdx = 0; outIdx < fieldMapping.length; outIdx++) {
			int inIdx = fieldMapping[outIdx];
			setField(rows, outIdx, fieldTypes.get(inIdx), batch.cols[inIdx]);
		}
	}

	private static void setField(Row[] rows, int fieldIdx, TypeDescription schema, ColumnVector vector) {


		switch (schema.getCategory()) {
			case BOOLEAN:
				if (vector.noNulls) {
					readNonNullBooleanColumn(rows, fieldIdx, (LongColumnVector) vector);
				} else {
					readBooleanColumn(rows, fieldIdx, (LongColumnVector) vector);
				}
				break;
			case BYTE:
				if (vector.noNulls) {
					readNonNullByteColumn(rows, fieldIdx, (LongColumnVector)vector);
				} else {
					readByteColumn(rows, fieldIdx, (LongColumnVector)vector);
				}
				break;
			case SHORT:
				if (vector.noNulls) {
					readNonNullShortColumn(rows, fieldIdx, (LongColumnVector)vector);
				} else {
					readShortColumn(rows, fieldIdx, (LongColumnVector)vector);
				}
				break;
			case INT:
				if (vector.noNulls) {
					readNonNullIntColumn(rows, fieldIdx, (LongColumnVector)vector);
					break;
				} else {
					readIntColumn(rows, fieldIdx, (LongColumnVector)vector);
				}
			case LONG:
				if (vector.noNulls) {
					readNonNullLongColumn(rows, fieldIdx, (LongColumnVector)vector);
					break;
				} else {
					readLongColumn(rows, fieldIdx, (LongColumnVector)vector);
				}
			case FLOAT:
				if (vector.noNulls) {
					readNonNullFloatColumn(rows, fieldIdx, (DoubleColumnVector)vector);
					break;
				} else {
					readFloatColumn(rows, fieldIdx, (DoubleColumnVector)vector);
				}
			case DOUBLE:
				if (vector.noNulls) {
					readNonNullDoubleColumn(rows, fieldIdx, (DoubleColumnVector)vector);
					break;
				} else {
					readDoubleColumn(rows, fieldIdx, (DoubleColumnVector)vector);
				}
			case CHAR:
			case VARCHAR:
			case STRING:
				if (vector.noNulls) {
					readNonNullStringColumn(rows, fieldIdx, (BytesColumnVector)vector);
				} else {
					readStringColumn(rows, fieldIdx, (BytesColumnVector)vector);
				}
				break;
			case DATE:
				if (vector.noNulls) {
					readNonNullDateColumn(rows, fieldIdx, (LongColumnVector)vector);
				} else {
					readDateColumn(rows, fieldIdx, (LongColumnVector)vector);
				}
				break;
			case TIMESTAMP:
				if (vector.noNulls) {
					readNonNullTimestampColumn(rows, fieldIdx, (TimestampColumnVector)vector);
				} else {
					readTimestampColumn(rows, fieldIdx, (TimestampColumnVector)vector);
				}
				break;
			case BINARY:
				if (vector.noNulls) {
					readNonNullBinaryColumn(rows, fieldIdx, (BytesColumnVector) vector);
				} else {
					readBinaryColumn(rows, fieldIdx, (BytesColumnVector) vector);
				}
				break;
			case STRUCT:
				if (vector.noNulls) {
					readNonNullStructColumn(rows, fieldIdx, (StructColumnVector) vector, schema);
				} else {
					readStructColumn(rows, fieldIdx, (StructColumnVector) vector, schema);
				}
				break;
			case LIST:
				if (vector.noNulls) {
					readNonNullListColumn(rows, fieldIdx, (ListColumnVector) vector, schema);
				}
				break;
			case MAP:
				if (vector.noNulls) {
					readNonNullMapColumn(rows, fieldIdx, (MapColumnVector) vector, schema);
				}
				break;
			case DECIMAL:
				if (vector.noNulls) {
					readNonNullDecimalColumn(rows, fieldIdx, (DecimalColumnVector) vector, schema);
				}
				break;
				//throw new UnsupportedOperationException("DECIMAL type not supported yet");
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	private static void setField(Row reuse, int fieldIdx, TypeDescription schema, ColumnVector vector, int row) {

		if (isFieldNull(vector, row)) {
			reuse.setField(fieldIdx, null);
		} else {
			switch (schema.getCategory()) {
				case BOOLEAN:
					reuse.setField(fieldIdx, ((LongColumnVector)vector).vector[row] != 0);
					break;
				case BYTE:
					reuse.setField(fieldIdx, (byte)((LongColumnVector)vector).vector[row]);
					break;
				case SHORT:
					reuse.setField(fieldIdx, (short)((LongColumnVector)vector).vector[row]);
					break;
				case INT:
					reuse.setField(fieldIdx, (int)((LongColumnVector)vector).vector[row]);
					break;
				case LONG:
					reuse.setField(fieldIdx, ((LongColumnVector)vector).vector[row]);
					break;
				case FLOAT:
					reuse.setField(fieldIdx, (float)((DoubleColumnVector)vector).vector[row]);
					break;
				case DOUBLE:
					reuse.setField(fieldIdx, ((DoubleColumnVector)vector).vector[row]);
					break;
				case CHAR:
				case VARCHAR:
				case STRING:
					reuse.setField(fieldIdx, readString(vector, row));
					break;
				case DATE:
					reuse.setField(fieldIdx, new Date(((LongColumnVector)vector).vector[row]));
					break;
				case TIMESTAMP:
					reuse.setField(fieldIdx, readTimestamp(vector, row));
					break;
				case BINARY:
					reuse.setField(fieldIdx, readBinary(vector, row, (byte[])reuse.getField(fieldIdx)));
					break;
				case STRUCT:
					List<TypeDescription> childrenTypes = schema.getChildren();
					int numChildren = childrenTypes.size();

					Row field = (Row)reuse.getField(fieldIdx);
					if (field == null) {
						field = new Row(numChildren);
					}

					StructColumnVector struct = (StructColumnVector) vector;
					for(int i = 0; i < numChildren; i++) {
						TypeDescription fieldType = childrenTypes.get(i);
						setField(field, i, fieldType, struct.fields[i], row);
					}

					// important to handle serialization
					if (field.getArity()==1) {
						System.out.println(field.getArity());
						reuse.setField(fieldIdx,field.getField(0));
					}
					else {
						reuse.setField(fieldIdx, field);
					}

					break;

				case LIST:

					TypeDescription fieldType = schema.getChildren().get(0);

					ListColumnVector list = (ListColumnVector) vector;

					int length = (int) list.lengths[row];
					int offset = (int) list.offsets[row];

					if (length > 0) {

						Row listfield = null;
						if (listfield == null) {
							listfield = new Row(length);
						}

						StructColumnVector structColumnVector = (StructColumnVector) list.child;
						for (int j = 0; j < length; j++) {
							setField(listfield,j,fieldType,structColumnVector,offset+j);
						}

						// important to handle serialization
						List list1 = new ArrayList();
						Object[] listObject = new Object[listfield.getArity()];
						for (int j = 0; j < listfield.getArity(); j++) {
							list1.add(j,listfield.getField(j));
							listObject[j] = listfield.getField(j);
						}
						//reuse.setField(fieldIdx,list1);
						reuse.setField(fieldIdx,listObject);
					}

					//throw new UnsupportedOperationException("LIST type not supported yet");
					break;
				case MAP:
					throw new UnsupportedOperationException("MAP type not supported yet");
				case DECIMAL:
					//throw new UnsupportedOperationException("DECIMAL type not supported yet");
				case UNION:
					//throw new UnsupportedOperationException("UNION type not supported yet");
				default:
					//throw new IllegalArgumentException("Unknown type " + schema);
			}
		}
	}

	/**
	 * Check if field is null.
	 *
	 * @param vector The column to read from.
	 * @param row The row to check.
	 * @return True if field is null, false otherwise.
	 */
	private static boolean isFieldNull(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		return (!vector.noNulls && vector.isNull[row]);
	}

	private static void readNonNullBooleanColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				row.setField(fieldIdx, vector.vector[0] != 0);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				rows[i].setField(fieldIdx, vector.vector[i] != 0);
			}
		}
	}

	private static void readNonNullByteColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				row.setField(fieldIdx, (byte) vector.vector[0]);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				rows[i].setField(fieldIdx, (byte) vector.vector[i]);
			}
		}
	}

	private static void readNonNullShortColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				row.setField(fieldIdx, (short) vector.vector[0]);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				rows[i].setField(fieldIdx, (short) vector.vector[i]);
			}
		}
	}

	private static void readNonNullIntColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				row.setField(fieldIdx, (int) vector.vector[0]);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				rows[i].setField(fieldIdx, (int) vector.vector[i]);
			}
		}
	}

	private static void readNonNullLongColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				row.setField(fieldIdx, vector.vector[0]);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				rows[i].setField(fieldIdx, vector.vector[i]);
			}
		}
	}

	private static void readNonNullFloatColumn(Row[] rows, int fieldIdx, DoubleColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				row.setField(fieldIdx, (float) vector.vector[0]);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				rows[i].setField(fieldIdx, (float) vector.vector[i]);
			}
		}
	}

	private static void readNonNullDoubleColumn(Row[] rows, int fieldIdx, DoubleColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				row.setField(fieldIdx, vector.vector[0]);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				rows[i].setField(fieldIdx, vector.vector[i]);
			}
		}
	}

	private static void readNonNullStringColumn(Row[] rows, int fieldIdx, BytesColumnVector bytes) {
		if (bytes.isRepeating) {
			String s = new String(bytes.vector[0], bytes.start[0], bytes.length[0]);
			for (Row row : rows) {
				row.setField(fieldIdx, s);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				//System.out.println("row index:"+i +" "+rows.length);
				//System.out.println("row index:"+i +" "+bytes.length[i]);
				if (bytes.length[i]>0)
					rows[i].setField(fieldIdx, new String(bytes.vector[i], bytes.start[i], bytes.length[i]));
				else
					rows[i].setField(fieldIdx, new String(""));
			}
		}
	}

	private static void readNonNullDateColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				Date reuse = (Date)row.getField(fieldIdx);
				Date d = (reuse != null) ? reuse : new Date(0);
				d.setTime(vector.vector[0]);
				row.setField(fieldIdx, d);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				Date reuse = (Date)rows[i].getField(fieldIdx);
				Date d = (reuse != null) ? reuse : new Date(0);
				d.setTime(vector.vector[i]);
				rows[i].setField(fieldIdx, d);
			}
		}
	}

	private static void readNonNullTimestampColumn(Row[] rows, int fieldIdx, TimestampColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				Timestamp reuse = (Timestamp)row.getField(fieldIdx);
				Timestamp ts = (reuse != null) ? reuse : new Timestamp(0);
				ts.setTime(vector.time[0]);
				ts.setNanos(vector.nanos[0]);
				row.setField(fieldIdx, ts);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				Timestamp reuse = (Timestamp)rows[i].getField(fieldIdx);
				Timestamp ts = (reuse != null) ? reuse : new Timestamp(0);
				ts.setTime(vector.time[i]);
				ts.setNanos(vector.nanos[i]);
				rows[i].setField(fieldIdx, ts);
			}
		}
	}

	private static void readNonNullBinaryColumn(Row[] rows, int fieldIdx, BytesColumnVector bytes) {

		if (bytes.isRepeating) {
			int length = bytes.length[0];
			for (Row row : rows) {
				byte[] reuse = (byte[]) row.getField(fieldIdx);
				byte[] result = (reuse != null && reuse.length == length) ? reuse : new byte[length];
				System.arraycopy(bytes.vector[0], bytes.start[0], result, 0, length);
				row.setField(fieldIdx, result);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {

				int length = bytes.length[i];
				//System.out.println(Arrays.toString(bytes.length));
				if(bytes.length[i] == 0 ){
					String a = "a";
					byte[] temp = a.getBytes();
					rows[i].setField(fieldIdx, temp);
				}
				else {
					byte[] reuse = (byte[])rows[i].getField(fieldIdx);
					byte[] result = (reuse != null && reuse.length == length) ? reuse : new byte[length];
					System.arraycopy(bytes.vector[i], bytes.start[i], result, 0, length);
					//rows[i].setField(fieldIdx, Arrays.asList(result));
					String a = "a";
					byte[] temp = a.getBytes();
					rows[i].setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readNonNullStructColumn(Row[] rows, int fieldIdx, StructColumnVector struct, TypeDescription schema) {
		System.out.println(" readNonNullStructColumn ");


		List<TypeDescription> childrenTypes = schema.getChildren();

		if (struct.isRepeating) {

			int numChildren = childrenTypes.size();
			// first child. Set rows
			for (int i = 0; i < rows.length; i++) {
				Row field = (Row) rows[i].getField(fieldIdx);
				if (field == null) {
					field = new Row(numChildren);
				}
				TypeDescription fieldType = childrenTypes.get(0);
				setField(field, 0, fieldType, struct.fields[0], 0);

				rows[i].setField(fieldIdx, field);
			}
			// following children. Row is set
			for (int j = 1; j < numChildren; j++) {
				TypeDescription fieldType = childrenTypes.get(j);

				for (int i = 0; i < rows.length; i++) {
					Row field = (Row) rows[i].getField(fieldIdx);
					setField(field, j, fieldType, struct.fields[j], 0);
				}
			}

		} else {
			int numChildren = childrenTypes.size();

			// first child. Set rows
			for (int i = 0; i < rows.length; i++) {
				Row field = (Row) rows[i].getField(fieldIdx);
				if (field == null) {
					field = new Row(numChildren);
				}
				TypeDescription fieldType = childrenTypes.get(0);
				setField(field, 0, fieldType, struct.fields[0], i);
				//rows[i].setField(fieldIdx, field);

				rows[i].setField(fieldIdx, field);
			}
			// following children. Row is set
			for (int j = 1; j < numChildren; j++) {
				TypeDescription fieldType = childrenTypes.get(j);

				for (int i = 0; i < rows.length; i++) {
					Row field = (Row) rows[i].getField(fieldIdx);
					setField(field, j, fieldType, struct.fields[j], i);
				}
			}
		}

	}

	private static void readNonNullListColumn(Row[] rows, int fieldIdx, ListColumnVector list, TypeDescription schema) {


		System.out.println(" readNonNullListColumn ");
		TypeDescription fieldType = schema.getChildren().get(0);
		System.out.println(fieldType);
		if (list.isRepeating) {

			System.out.println("Repeating Error");

		} else {

			for (int i = 0; i < rows.length && i < 4; i++) {


				int length = (int) list.lengths[i];
				int offset = (int) list.offsets[i];

				if (length > 0) {

					Row field = null;
					if (field == null) {
						field = new Row(length);
					}
					StructColumnVector structColumnVector = (StructColumnVector) list.child;

					for (int j = 0; j < length; j++) {
						setField(field,j,fieldType,structColumnVector,offset+j);
					}
					System.out.println(field.getArity());

					// important for serialization
					List list1 = new ArrayList();
					Object[] list2 = new Object[field.getArity()];
					for (int j = 0; j < field.getArity(); j++) {
						list1.add(j,field.getField(j));
						list2[j]=field.getField(j);
					}
					//rows[i].setField(fieldIdx,list1);
					rows[i].setField(fieldIdx,list2);

				}
			}

			//reuse.setField(fieldIdx, field1);

		}

	}

	private static void readNonNullMapColumn(Row[] rows, int fieldIdx, MapColumnVector map, TypeDescription schema) {


		System.out.println("readNonNullMapColumn");
		List<TypeDescription> fieldType = schema.getChildren();
		TypeDescription keyType = fieldType.get(0);
		TypeDescription valueType = fieldType.get(1);

		if (map.isRepeating) {

			System.out.println("Repeating Error");

		} else {


			for (int i = 0; i < rows.length && i < 5; i++) {

				int length = (int) map.lengths[i];
				int offset = (int) map.offsets[i];

				if (length > 0) {

					Row keyField = null;
					Row valuesField = null;
					if (keyField == null) {
						keyField = new Row(length);
						valuesField = new Row(length);
					}

					ColumnVector keys = map.keys;
					ColumnVector values = map.values;

					for (int j = 0; j < length; j++) {
						setField(keyField,j,keyType,keys,offset+j);
						setField(valuesField,j,valueType,values,offset+j);
					}

					Map<Object,Object> map1 = new HashMap<Object,Object>(length);
					for (int j = 0; j < length; j++) {
						map1.put(keyField.getField(j),valuesField.getField(j));
					}

					rows[i].setField(fieldIdx,map1);
				}
			}

		}

	}


	private static void readNonNullDecimalColumn(Row[] rows, int fieldIdx, DecimalColumnVector vector, TypeDescription schema) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				row.setField(fieldIdx, vector.vector[0]);
			}
		} else {
			for (int i = 0; i < rows.length; i++) {

				HiveDecimalWritable hiveDecimalWritablel = vector.vector[i];
				HiveDecimal hiveDecimal = hiveDecimalWritablel.getHiveDecimal();

				BigDecimal bigDecimal = hiveDecimal.bigDecimalValue();
				rows[i].setField(fieldIdx, bigDecimal);
			}
		}
	}
	static OrcList nextList(ColumnVector vector,
							int row,
							TypeDescription schema,
							Object previous) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			OrcList result;
			List<TypeDescription> childrenTypes = schema.getChildren();
			TypeDescription valueType = childrenTypes.get(0);
			if (previous == null ||
				previous.getClass() != ArrayList.class) {
				result = new OrcList(schema);
			} else {
				result = (OrcList) previous;
			}
			ListColumnVector list = (ListColumnVector) vector;
			int length = (int) list.lengths[row];
			int offset = (int) list.offsets[row];
			result.ensureCapacity(length);
			int oldLength = result.size();
			int idx = 0;
			/*
			while (idx < length && idx < oldLength) {
				result.set(idx, nextValue(list.child, offset + idx, valueType,
					result.get(idx)));
				idx += 1;
			}
			if (length < oldLength) {
				for(int i= oldLength - 1; i >= length; --i) {
					result.remove(i);
				}
			} else if (oldLength < length) {
				while (idx < length) {
					result.add(nextValue(list.child, offset + idx, valueType, null));
					idx += 1;
				}
			}
			*/
			return result;
		} else {
			return null;
		}
	}

	private static void readBooleanColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
		if (vector.isRepeating) {
			for (Row row : rows) {
				if (vector.isNull[0]) {
					row.setField(fieldIdx, null);
				} else {
					row.setField(fieldIdx, vector.vector[0] != 0);
				}
			}
		} else {
			for (int i = 0; i < rows.length; i++) {
				if (vector.isNull[i]) {
					rows[i].setField(fieldIdx, null);
				} else {
					rows[i].setField(fieldIdx, vector.vector[i] != 0);
				}
			}
		}
	}

	private static void readByteColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
//		if (vector.isRepeating) {
//			for (Row row : rows) {
//				row.setField(fieldIdx, (byte) vector.vector[0]);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				rows[i].setField(fieldIdx, (byte) vector.vector[i]);
//			}
//		}
	}

	private static void readShortColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
//		if (vector.isRepeating) {
//			for (Row row : rows) {
//				row.setField(fieldIdx, (short) vector.vector[0]);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				rows[i].setField(fieldIdx, (short) vector.vector[i]);
//			}
//		}
	}

	private static void readIntColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
//		if (vector.isRepeating) {
//			for (Row row : rows) {
//				row.setField(fieldIdx, (int) vector.vector[0]);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				rows[i].setField(fieldIdx, (int) vector.vector[i]);
//			}
//		}
	}

	private static void readLongColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
//		if (vector.isRepeating) {
//			for (Row row : rows) {
//				row.setField(fieldIdx, vector.vector[0]);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				rows[i].setField(fieldIdx, vector.vector[i]);
//			}
//		}
	}

	private static void readFloatColumn(Row[] rows, int fieldIdx, DoubleColumnVector vector) {
//		if (vector.isRepeating) {
//			for (Row row : rows) {
//				row.setField(fieldIdx, (float) vector.vector[0]);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				rows[i].setField(fieldIdx, (float) vector.vector[i]);
//			}
//		}
	}

	private static void readDoubleColumn(Row[] rows, int fieldIdx, DoubleColumnVector vector) {
//		if (vector.isRepeating) {
//			for (Row row : rows) {
//				row.setField(fieldIdx, vector.vector[0]);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				rows[i].setField(fieldIdx, vector.vector[i]);
//			}
//		}
	}

	private static void readStringColumn(Row[] rows, int fieldIdx, BytesColumnVector bytes) {
//		if (bytes.isRepeating) {
//			String s = new String(bytes.vector[0], bytes.start[0], bytes.length[0]);
//			for (Row row : rows) {
//				row.setField(fieldIdx, s);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				rows[i].setField(fieldIdx, new String(bytes.vector[i], bytes.start[i], bytes.length[i]));
//			}
//		}
	}

	private static void readDateColumn(Row[] rows, int fieldIdx, LongColumnVector vector) {
//		if (vector.isRepeating) {
//			for (Row row : rows) {
//				Date reuse = (Date)row.getField(fieldIdx);
//				Date d = (reuse != null) ? reuse : new Date(0);
//				d.setTime(vector.vector[0]);
//				row.setField(fieldIdx, d);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				Date reuse = (Date)rows[i].getField(fieldIdx);
//				Date d = (reuse != null) ? reuse : new Date(0);
//				d.setTime(vector.vector[i]);
//				rows[i].setField(fieldIdx, d);
//			}
//		}
	}

	private static void readTimestampColumn(Row[] rows, int fieldIdx, TimestampColumnVector vector) {
//		if (vector.isRepeating) {
//			for (Row row : rows) {
//				Timestamp reuse = (Timestamp)row.getField(fieldIdx);
//				Timestamp ts = (reuse != null) ? reuse : new Timestamp(0);
//				ts.setTime(vector.time[0]);
//				ts.setNanos(vector.nanos[0]);
//				row.setField(fieldIdx, ts);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				Timestamp reuse = (Timestamp)rows[i].getField(fieldIdx);
//				Timestamp ts = (reuse != null) ? reuse : new Timestamp(0);
//				ts.setTime(vector.time[i]);
//				ts.setNanos(vector.nanos[i]);
//				rows[i].setField(fieldIdx, ts);
//			}
//		}
	}

	private static void readBinaryColumn(Row[] rows, int fieldIdx, BytesColumnVector bytes) {
//		if (bytes.isRepeating) {
//			int length = bytes.length[0];
//			for (Row row : rows) {
//				byte[] reuse = (byte[]) row.getField(fieldIdx);
//				byte[] result = (reuse != null && reuse.length == length) ? reuse : new byte[length];
//				System.arraycopy(bytes.vector[0], bytes.start[0], result, 0, length);
//				row.setField(fieldIdx, result);
//			}
//		} else {
//			for (int i = 0; i < rows.length; i++) {
//				int length = bytes.length[i];
//				byte[] reuse = (byte[])rows[i].getField(fieldIdx);
//				byte[] result = (reuse != null && reuse.length == length) ? reuse : new byte[length];
//				System.arraycopy(bytes.vector[i], bytes.start[i], result, 0, length);
//				rows[i].setField(fieldIdx, result);
//			}
//		}
	}

	private static void readStructColumn(Row[] rows, int fieldIdx, StructColumnVector struct, TypeDescription schema) {
//		if (struct.isRepeating) {
//
//			List<TypeDescription> childrenTypes = schema.getChildren();
//			int numChildren = childrenTypes.size();
//			// first child. Set rows
//			for (int i = 0; i < rows.length; i++) {
//				Row field = (Row) rows[i].getField(fieldIdx);
//				if (field == null) {
//					field = new Row(numChildren);
//				}
//				TypeDescription fieldType = childrenTypes.get(0);
//				setField(field, 0, fieldType, struct.fields[0], 0);
//				rows[i].setField(fieldIdx, field);
//			}
//			// following children. Row is set
//			for (int j = 1; j < numChildren; j++) {
//				TypeDescription fieldType = childrenTypes.get(j);
//
//				for (int i = 0; i < rows.length; i++) {
//					Row field = (Row) rows[i].getField(fieldIdx);
//					setField(field, j, fieldType, struct.fields[j], 0);
//				}
//			}
//
//		} else {
//			List<TypeDescription> childrenTypes = schema.getChildren();
//			int numChildren = childrenTypes.size();
//			// first child. Set rows
//			for (int i = 0; i < rows.length; i++) {
//				Row field = (Row) rows[i].getField(fieldIdx);
//				if (field == null) {
//					field = new Row(numChildren);
//				}
//				TypeDescription fieldType = childrenTypes.get(0);
//				setField(field, 0, fieldType, struct.fields[0], i);
//				rows[i].setField(fieldIdx, field);
//			}
//			// following children. Row is set
//			for (int j = 1; j < numChildren; j++) {
//				TypeDescription fieldType = childrenTypes.get(j);
//
//				for (int i = 0; i < rows.length; i++) {
//					Row field = (Row) rows[i].getField(fieldIdx);
//					setField(field, j, fieldType, struct.fields[j], i);
//				}
//			}
//		}
	}


	private static String readString(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		BytesColumnVector bytes = (BytesColumnVector) vector;
		if (bytes.length[row] > 0) {
			return new String(bytes.vector[row], bytes.start[row], bytes.length[row]);
		}
		return new String();
	}

	private static Timestamp readTimestamp(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		TimestampColumnVector tcv = (TimestampColumnVector) vector;
		Timestamp ts = new Timestamp(tcv.time[row]);
		ts.setNanos(tcv.nanos[row]);
		return ts;
	}

	private static byte[] readBinary(ColumnVector vector, int row, byte[] reuse) {
		if (vector.isRepeating) {
			row = 0;
		}
		BytesColumnVector bytes = (BytesColumnVector) vector;
		int length = bytes.length[row];
		byte[] result = (reuse != null && reuse.length == length) ? reuse : new byte[length];
		System.arraycopy(bytes.vector[row], bytes.start[row], result, 0, length);
		return result;
	}

}
