use arrow::{
    array::RecordBatch, datatypes::DataType, error::ArrowError,
    util::display::array_value_to_string,
};

pub fn pretty_batches(batches: &Vec<RecordBatch>) {
    println!("{batches:#?}")
}

pub fn record_batch_to_string(batch: &RecordBatch) -> Result<String, ArrowError> {
    let mut output = String::new();
    for row in 0..batch.num_columns() {
        for col in 0..batch.num_columns() {
            if col != 0 {
                output.push(' ');
            }
            let column = batch.column(col);

            if column.is_null(row) {
                output.push_str("NULL");
                continue;
            }

            let string = array_value_to_string(column, row)?;

            if *column.data_type() == DataType::UInt8 && string.is_empty() {
                output.push_str("{empty}");
                continue;
            }
            output.push_str(&string);
        }
        output.push('\n');
    }
    Ok(output)
}

#[cfg(test)]
mod util_test {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::error::ArrowError;
    use arrow::record_batch::RecordBatch;

    use crate::utill::record_batch_to_string;

    fn build_record_batch() -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("first_name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Bill", "Gregg", "John"])),
            ],
        )?;
        Ok(batch)
    }

    #[test]
    fn test_record_batch_to_string() -> Result<(), ArrowError> {
        let record_batch = build_record_batch()?;
        let output = record_batch_to_string(&record_batch)?;

        let expected = vec!["1 Bill", "2 Gregg", "3 John"];
        let actual: Vec<&str> = output.lines().collect();
        assert_eq!(expected, actual);

        Ok(())
    }
}
