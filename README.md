#About
A library for building custom [Hive](http://hive.apache.org/ "Apache - Welcome to Hive!") [SerDes](https://cwiki.apache.org/confluence/display/Hive/SerDe "Apache Hive Wiki - SerDe").

#Start using
You can [download](https://github.com/lastfm/hive-serde/downloads) a JAR file or obtain hive-serde from Maven Central using the following identifier:

* [fm.last.commons.hive.serde:lastcommons-hive-serde:2.0.0](http://search.maven.org/#artifactdetails%7Cfm.last.commons.hive.serde%7Clastcommons-hive-serde%7C2.0.0%7Cjar)

#Features
* Provides a very simple API for materializing your custom record types into Hive rows.
* Field name mapping.
* Simultaneous mapping of fields to multiple columns of different types.

#Writing your own SerDe
1. Define your fields with implementations of `Field` - this is best done with an enum.
2. Create a `FieldFactory` implementation - best to do this with an inner static enum inside your `Field` enum.
3. Create a subclass of `AbstractReadOnlySerDe` that takes in instances of your data file's record type.
4. Optionally create any `FieldTransform`s for mapping custom types.
5. Map your record type to fields with an implementation of `AbstractReadOnlySerDe<R>#mapRecordIntoRow(R writable, Row row)`.

#Example
###SerDe implementation
        public class CustomDataFileSerDe extends AbstractReadOnlySerDe<CustomRecord> {
     
          public CustomDataFileSerDe() {
            super(CustomRecord.class, CustomRecordField.Factory.INSTANCE);
          }
     
          public void mapRecordIntoRow(CustomRecord record, Row row) throws SerDeException {
            row.setField(CustomRecordField.EVENT_ID, record.getId());
            row.setField(CustomRecordField.TIMESTAMP, record.getTime());
            row.setField(CustomRecordField.TYPE, record.getType());
          }     
        }
See [`fm.last.commons.hive.serde.TestField`](https://github.com/lastfm/hive-serde/blob/master/src/test/java/fm/last/commons/hive/serde/TestField.java "GitHub - TestField source") for an example `Field` and `FieldFactory` implementation - they are very simple.
###Example field transform
Lets assume that our records encode an event's type as `1 = Bang` and `2 = Fizzle` - if the destination column type is a string we might wish to convert the code to something more readable:

        public class EventTypeFieldTransform extends AbstractStringFieldTransform implements FieldTransform {
        
          public Object transform(Object value, TypeInfo columnType) {
            if (isColumnOfStringType(columnType)) {
              int eventTypeCode = ((Integer) value).intValue();
              switch (eventTypeCode) {
                case 1:
                  return "Bang";
                case 2:
                  return "Fizzle";
                default:
                  return "Unknown";
              }
            }
            return eventTypeCode;
          }    
        }
###Hive table definition
        CREATE EXTERNAL TABLE IF NOT EXISTS event_data (
          event_id int,
          timestamp string,
          type string
        )
        ROW FORMAT SERDE 'org.my.project.CustomDataFileSerDe'
        STORED AS
          INPUTFORMAT 'org.my.project.MyCustomRecordProducingInputFormat'
          OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat'
        LOCATION '...';
###Mapping between different column and field names
Add the mapping when declaring the Hive table like so:

        WITH SERDEPROPERTIES ('time' = 'timestamp', ...

Or to simultaneous mapping of fields to multiple columns of different types:

        WITH SERDEPROPERTIES ('event_type_as_string' = 'type', 'event_type_code' = 'type', ...

#Building
This project uses the [Maven](http://maven.apache.org/) build system.

#Acknowledgements
Roberto Congiu for his posts on writing SerDes ([1](http://www.congiu.com/writing-a-hive-serde-for-lwes-event-files/ "Writing a SerDe in Hive for Lwes event files"), [2](http://www.congiu.com/a-json-readwrite-serde-for-hive/ "A JSON read/write SerDe for Hive")).

#Legal
Copyright 2012 [Last.fm](http://www.last.fm/)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
