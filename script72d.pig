emp_rel = LOAD '/home/acadgild/hadoop/hdfs/employee_details.txt' USING PigStorage(',') AS (emp_id:int, emp_name:chararray, emp_salary:int, emp_rating:int);

emp_expense_rel = LOAD '/home/acadgild/hadoop/hdfs/employee_expenses.txt' AS (emp_id:int, emp_expense:int);

joined_rel = JOIN emp_rel BY emp_id, emp_expense_rel BY emp_id;

final_rel = FOREACH joined_rel GENERATE emp_rel::emp_id , emp_rel::emp_name;

DUMP final_rel;

