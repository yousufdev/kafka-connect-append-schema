Kafka Connect SMT to append schema to a record

This SMT supports appending a schema into the record Value
Property:

|Name|Description|Type|Condition|Importance|
|---|---|---|---|---|
|`schema`| Field name for schema | String | `schema must contain valid json but with single quotes used in json as shown in the example below` | High |

Example on how to add to your connector:
```
"transforms": "appendSchema",
"transforms.appendSchema.type": "com.github.yousufdev.kafka.connect.smt.AppendSchema$Value",
"transforms.appendSchema.schema" : "{'schema':{'type':'struct','fields':[{'type':'int64','optional':false,'field':'id'},{'type':'string','optional':true,'field':'name'},{'type':'string','optional':true,'field':'email'},{'type':'string','optional':true,'field':'department'}],'optional':false,'name':'test'}}"
```

Lots borrowed from the Apache Kafka® `InsertField` SMT