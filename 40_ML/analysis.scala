// load data
val customer_raw = spark.read.csv("/tmp/ftp_input/customer/*.csv");

customer_raw.show();

val customer = customer_raw.select($"_c0".alias("customerid"), $"_c1".alias("name"), $"_c2".alias("namekana"), $"_c3".alias("zipcode"), $"_c4".alias("address")) ;

customer.show();

// filter : 301-0で始まる行
val filtered = customer.filter ($"zipcode".contains("301-0"));
filtered.show();

// output
filtered.write.parquet("/tmp/filtered_customer");
