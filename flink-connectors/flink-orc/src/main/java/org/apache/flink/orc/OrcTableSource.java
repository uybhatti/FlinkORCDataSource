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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

public class OrcTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

	private String path;
	private TypeDescription orcSchema;
	private RowTypeInfo typeInfo;
	private Configuration orcConfig;
	private int[] fieldMapping;

	public OrcTableSource(String path, String orcSchema) {
		this(path, orcSchema, new Configuration());
	}

	public OrcTableSource(String path, String orcSchema, Configuration orcConfig) {
		this(path, TypeDescription.fromString(orcSchema), orcConfig);
	}

	public OrcTableSource(String path, TypeDescription orcSchema, Configuration orcConfig) {
		this.path = path;
		this.orcSchema = orcSchema;
		this.orcConfig = orcConfig;

		this.typeInfo = (RowTypeInfo) OrcUtils.schemaToTypeInfo(this.orcSchema);
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {

		RowOrcInputFormat orcIF = new RowOrcInputFormat(path, orcSchema, orcConfig);
		if (fieldMapping != null) {
			orcIF.setFieldMapping(fieldMapping);
		}

		return execEnv
			.createInput(orcIF);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return typeInfo;
	}

	@Override
	public ProjectableTableSource<Row> projectFields(int[] fields) {

		OrcTableSource copy = new OrcTableSource(path, orcSchema, orcConfig);

		// set field mapping
		copy.fieldMapping = fields;

		// adapt TypeInfo
		TypeInformation[] fieldTypes = new TypeInformation[fields.length];
		String[] fieldNames = new String[fields.length];
		for (int i = 0; i < fields.length; i++) {
			fieldTypes[i] = this.typeInfo.getTypeAt(fields[i]);
			fieldNames[i] = this.typeInfo.getFieldNames()[fields[i]];
		}
		copy.typeInfo = new RowTypeInfo(fieldTypes, fieldNames);

		return copy;
	}
}
