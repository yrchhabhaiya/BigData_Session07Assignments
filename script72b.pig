base_rel = LOAD '/home/acadgild/hadoop/hdfs/employee_details.txt' USING PigStorage(',') AS (emp_id:int, emp_name:chararray, emp_salary:int, emp_rating:int);

filter_base_rel = filter base_rel by emp_id%2==1;

sorted_base_rel = order filter_base_rel by emp_name asc;

final_sorted_base_rel = order sorted_base_rel by emp_salary desc;

final_rel = foreach final_sorted_base_rel generate emp_id , emp_name;

limit_rel = limit final_sorted_base_rel 3;

dump limit_rel;
