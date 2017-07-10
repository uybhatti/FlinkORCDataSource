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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.Row;
import org.junit.Test;
import scala.tools.cmd.gen.AnyVals;

import java.util.List;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;

public class OrcInputFormatITCase extends MultipleProgramsTestBase {

	public OrcInputFormatITCase() {
		super(TestExecutionMode.CLUSTER);
	}

	//@Test
	public void testOrcTS() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		OrcTableSource orc = new OrcTableSource(
			"/users/fhueske/orcTest",
			"struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>"
		);

		CsvTableSource csv = new CsvTableSource(
			"/users/fhueske/blubb",
			new String[]{"_col0","_col1","_col2","_col3","_col4","_col5","_col6","_col7","_col8"},
			new TypeInformation[]{INT_TYPE_INFO, STRING_TYPE_INFO,STRING_TYPE_INFO,STRING_TYPE_INFO,INT_TYPE_INFO,STRING_TYPE_INFO,INT_TYPE_INFO,INT_TYPE_INFO,INT_TYPE_INFO});

		tEnv.registerTableSource("orcTable", orc);
		tEnv.registerTableSource("csvTable", csv);

		Table t = tEnv.scan("orcTable")
			.groupBy("_col2")
			.select("_col2, COUNT(_col1)");
//		.select("COUNT(_col0)");

//		env.execute();

		tEnv.toDataSet(t, Row.class).print();

	}

	@Test
	public void testOrcNestedData() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		String path = "/Users/usmanyounas/Desktop/Lib/orc/examples/TestOrcFile.test1.orc";
		OrcTableSource orc = new OrcTableSource(
			path,
			"struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int,long1:bigint,float1:float,double1:double,bytes1:binary,string1:string,middle:struct<list:array<struct<int1:int,string1:string>>>,list1:array<struct<int1:int,string1:string>>,map:map<string,struct<int1:int,string1:string>>>"
		);



		tEnv.registerTableSource("orcTable", orc);

		Table t = tEnv.scan("orcTable");

		System.out.println(t.getSchema());

		//t = tEnv.sql("select middle,list,map FROM orcTable");
		t = tEnv.scan("orcTable")
			.select("middle,list1.at(1),map");
			//.where("middle. > 0");
//		.select("COUNT(_col0)");

		System.out.println("Schema Me"+t.getSchema());
//		env.execute();


		tEnv.toDataSet(t,Row.class).print();
		/*
		DataSet<Row> dataSet = tEnv.toDataSet(t,Row.class);
		List<Row> list = dataSet.collect();
		*/
		/*
		for (int i=0; i<list.size();i++) {
			Row row = list.get(i);
			for (int j=0;j<row.getArity();j++) {
				Object object = row.getField(j);
				System.out.println(object);
				if (object!=null)
					System.out.println(object.getClass());
				else
					System.out.println("null");
			}

		}
*/

		/*
		Row record = list.get(1);
		Row structField = (Row) record.getField(0);
		List listField = (List) record.getField(1);
		Map<Object,Object> mapField = (Map<Object,Object>)record.getField(2);

		System.out.println("structField:"+structField);
		System.out.println("listField:"+listField.get(1));
		System.out.println("mapField:"+mapField.get("chani"));
		*/



		//System.out.println(list.);
		//tEnv.toDataSet(t, Row.class).print();

	}

	//@Test
	public void testOrcDecimal() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		String path = "/Users/usmanyounas/Desktop/Lib/orc/examples/decimal.orc";
		OrcTableSource orc = new OrcTableSource(
			path,
			"struct<_col0:decimal(10,5)>"
		);



		tEnv.registerTableSource("orcTable", orc);

		Table t = tEnv.scan("orcTable")
			.select("*")
			.where("_col0 > 1999");
//		.select("COUNT(_col0)");

//		env.execute();

		//DataSet<Row> dataSet = tEnv.toDataSet(t,Row.class);
		//List<Row> list = dataSet.collect();


		//System.out.println(list.);
		tEnv.toDataSet(t, Row.class).print();


	}


	//@Test
	public void testOrcTS3() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		String path = "/Users/usmanyounas/Desktop/Lib/orc/examples/demo-11-none.orc";
		OrcTableSource orc = new OrcTableSource(
			path,
			"struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>"
		);



		tEnv.registerTableSource("orcTable", orc);

		Table t = tEnv.scan("orcTable")
			.select("*");

		tEnv.toDataSet(t, Row.class).print();



	}

	//@Test
	public void testOrcTS4() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		String path = "/Users/usmanyounas/Desktop/Lib/orc/examples/demo-12-zlib.orc";
		OrcTableSource orc = new OrcTableSource(
			path,
			"struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>"
		);



		tEnv.registerTableSource("orcTable", orc);

		Table t = tEnv.scan("orcTable")
			.select("*");

		tEnv.toDataSet(t, Row.class).print();


	}
}
