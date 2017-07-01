emp_rel = LOAD '/home/acadgild/hadoop/hdfs/employee_details.txt' USING PigStorage(',') AS (emp_id:int, emp_name:chararray, emp_salary:int, emp_rating:int);

emp_expense_rel = LOAD '/home/acadgild/hadoop/hdfs/employee_expenses.txt' AS (emp_id:int, emp_expense:int);

joined_data = join emp_rel by emp_id FULL OUTER, emp_expense_rel by emp_id;

filtered_rel = FILTER joined_data BY emp_expense_rel::emp_id is null ;

final_rel = FOREACH filtered_rel GENERATE emp_rel::emp_id , emp_rel::emp_name;

DUMP final_rel;
