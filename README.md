# data_engineering_aws_glue
1. create s3 bucket 

2. insert landing jsons 

3. create athena tables for landing (customer, accelerometer,step_trainer) - criteria 2
    customer_landing.sql
    accelerometer_landing.sql
    step_trainer_landing.sql 

4. run transformation from landing to trusted - criteria 1
    customer_landing_to_trusted.py
    accelerometer_landing_to_trusted.py
    step_trainer_trusted.py

5. take pictures of landing - criteria 3 
    customer_landing.png
    customer_landing_details.png
    accelerometer_landing.png
    step_trainer_landing.png 

6. take pictures of trusted - criteria4   

7. create athena tables for trusted (customer_trusted,accelerometer_trusted,step_trainer_trusted)

8. run transformation from trusted to curated (customer_curated) 
    customer_trusted_to_curated.py has a node that inner joins the customer_trusted data with the accelerometer_trusted data by emails. The produced table should have only columns from the customer table. ??????
    customer_trusted_to_curated.py (just copy since it seems exactly as customer_trusted) 

9. run transformation from trusted to curated (machine_learning)
    machine_learning_curated.py 
