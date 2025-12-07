# Pyspark Assignment :
# 1. Question: 
# 1.1 Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data

<img width="1480" height="926" alt="Screenshot 2025-12-07 095516" src="https://github.com/user-attachments/assets/8804b88e-b1e1-4ecf-b185-7f2b3a50f7cb" />

# 1.2 Find the customers who have bought only iphone13 

<img width="1058" height="267" alt="Screenshot 2025-12-07 113609" src="https://github.com/user-attachments/assets/a465a04c-0f4d-44da-8110-1a93397d282f" />

# 1.3 Find customers who upgraded from product iphone13 to product iphone14 

<img width="778" height="324" alt="Screenshot 2025-12-07 113721" src="https://github.com/user-attachments/assets/1378bef2-1d57-4643-92a9-96b75d33f00c" />

# 2.Question: 
DataSet:Column(“card_number”) 

```("1234567891234567",), 
("5678912345671234",), 
("9123456712345678",), 
("1234567812341122",), 
("1234567812341342",)
```
# 2.1 Create a Dataframe as credit_card_df with different read methods 

<img width="876" height="480" alt="Screenshot 2025-12-07 113841" src="https://github.com/user-attachments/assets/89eaf799-4c36-43af-8286-94d98d290548" />

# 2.2 print number of partitions 

<img width="903" height="161" alt="Screenshot 2025-12-07 113902" src="https://github.com/user-attachments/assets/337072e7-408a-4181-a1dc-e7f7cc838b19" />

# 2.3 Increase the partition size to 5 

<img width="854" height="207" alt="Screenshot 2025-12-07 113924" src="https://github.com/user-attachments/assets/97af565c-1e6c-47a4-b7b2-78772e60b16c" />

# 2.4 Decrease the partition size back to its original partition size 

<img width="1014" height="147" alt="Screenshot 2025-12-07 115031" src="https://github.com/user-attachments/assets/ac60539f-0b8d-432c-bb61-0788f9692512" />

# 2.5 Create a UDF to print only the last 4 digits marking the remaining digits as * Eg: ************456

<img width="924" height="373" alt="Screenshot 2025-12-07 115048" src="https://github.com/user-attachments/assets/5ed20753-09a8-4553-97b1-7c45fe5b0f7d" />

# 3. Question: 

# 3.1 Create a Data Frame with custom schema creation by using Struct Type and Struct Field 

<img width="853" height="824" alt="Screenshot 2025-12-07 120012" src="https://github.com/user-attachments/assets/db5d7ecf-2aa7-4490-92ca-a91331a2c241" />

# 3.2 Column names should be log_id, user_id, user_activity, time_stamp using dynamic function 

<img width="729" height="562" alt="Screenshot 2025-12-07 123105" src="https://github.com/user-attachments/assets/4af0f2cc-a352-4eba-8884-10d9c6b0830c" />

# 3.3 Write a query to calculate the number of actions performed by each user in the last 7 days 

<img width="778" height="401" alt="Screenshot 2025-12-07 123126" src="https://github.com/user-attachments/assets/9fe1483b-efaa-4a21-b227-8e96a54ed98f" />

# 3.4 Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type 

<img width="728" height="410" alt="Screenshot 2025-12-07 131159" src="https://github.com/user-attachments/assets/a5c9b167-863a-4f30-b711-173b805b6799" />

# 4. Question 

# 4.1 Read JSON file provided in the attachment using the dynamic function 

<img width="1022" height="169" alt="Screenshot 2025-12-07 133540" src="https://github.com/user-attachments/assets/dc0fc74f-74a0-4fe0-a81d-3f770f1b8050" />

# Output:

<img width="792" height="123" alt="Screenshot 2025-12-07 133647" src="https://github.com/user-attachments/assets/cd74dcfd-d7ef-45e0-887d-d7afb5b46450" />

# 4.2 flatten the data frame which is a custom schema 

<img width="910" height="99" alt="Screenshot 2025-12-07 134344" src="https://github.com/user-attachments/assets/3cdf66d4-4765-45ba-b48e-a150479d8170" />

# Output:

<img width="608" height="231" alt="Screenshot 2025-12-07 134354" src="https://github.com/user-attachments/assets/d680f374-ecd3-4637-b917-4f10f0bcec15" />

# 4.3 find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count) 

<img width="559" height="74" alt="Screenshot 2025-12-07 134404" src="https://github.com/user-attachments/assets/9f8ecc90-8e90-4edd-a094-69af65b2ec74" />

# Output:

<img width="328" height="67" alt="Screenshot 2025-12-07 134409" src="https://github.com/user-attachments/assets/49dd5b97-8942-488c-88fc-7c93e4ed8aa5" />

# 4.4 Differentiate the difference using explode, explode outer, posexplode functions 

<img width="945" height="848" alt="Screenshot 2025-12-07 134441" src="https://github.com/user-attachments/assets/00424ef5-1280-4c59-8ceb-aa9847c70f32" />

# 4.5 Filter the id which is equal to 0001  

<img width="752" height="274" alt="Screenshot 2025-12-07 135323" src="https://github.com/user-attachments/assets/95f85a52-41a5-4c02-be84-bdafb843a8c0" />

# 4.6 Add a new column named load_date with the current date 

<img width="706" height="312" alt="Screenshot 2025-12-07 135452" src="https://github.com/user-attachments/assets/6125225a-07a4-4d8d-bc04-9e490eafa1f8" />

# 4.7 create 3 new columns as year, month, and day from the load_date column 

<img width="898" height="406" alt="Screenshot 2025-12-07 141315" src="https://github.com/user-attachments/assets/1d46cd4a-7c12-4219-b905-9aa5934715bf" />

# 5. Question 

# 5.1 create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way 

<img width="1240" height="905" alt="Screenshot 2025-12-07 142354" src="https://github.com/user-attachments/assets/b59dc878-0a81-4f1f-97dc-81fe518d6cd0" />

# 5.2 Find avg salary of each department 

<img width="1117" height="307" alt="Screenshot 2025-12-07 143206" src="https://github.com/user-attachments/assets/9806ad1c-b46e-4096-9426-c6f18bba7ae6" />

# 5.3 Find the employee’s name and department name whose name starts with ‘m’  

<img width="1146" height="317" alt="Screenshot 2025-12-07 143220" src="https://github.com/user-attachments/assets/43b385cc-590b-4cf7-bfdf-92da32c4a4e9" />

# 5.4 Create another new column in  employee_df as a bonus by multiplying employee salary *2 

<img width="972" height="433" alt="Screenshot 2025-12-07 143231" src="https://github.com/user-attachments/assets/489cb75d-ca35-48aa-86d2-435d26175834" />

# 5.5 Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department) 

<img width="1199" height="435" alt="Screenshot 2025-12-07 143432" src="https://github.com/user-attachments/assets/e8634e0f-ae8f-4e92-bd30-16c34e4d706b" />

# 5.6 Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way 

<img width="1187" height="711" alt="Screenshot 2025-12-07 143452" src="https://github.com/user-attachments/assets/6887b359-635a-43e8-ab7e-b07ac90069e8" />

<img width="849" height="305" alt="Screenshot 2025-12-07 143618" src="https://github.com/user-attachments/assets/0b54bd5a-61c6-4b79-9f51-69d6f8f2edd6" />

# 5.7 Derive a new data frame with country_name instead of State in employee_df  Eg(11,“james”,”D101”,”newyork”,8900,32) 

<img width="1208" height="170" alt="Screenshot 2025-12-07 144327" src="https://github.com/user-attachments/assets/3b80fed5-0b5a-4451-85df-9f333871a5ce" />
