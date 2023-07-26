-- A. Customer Nodes Exploration
/* 1. How many unique nodes are there on the Data Bank system? */
select count(distinct node_id) as unique_nodes
from customer_nodes;

/* 2. What is the number of nodes per region? */
select region_name, count(node_id) as total
from customer_nodes n
inner join regions r using(region_id)
group by 1
order by 2 desc;

/* 3. How many customers are allocated to each region? */
select region_name, count(distinct customer_id) as total_customers
from customer_nodes n
inner join regions r using(region_id)
group by 1
order by 2 desc;

/* 4. How many days on average are customers reallocated to a different node? */
select round(avg(datediff(end_date, start_date))) as average_days
from customer_nodes
where end_date <> "9999-12-31";

/* 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region? */
with cte1 as (
	select n.region_id, region_name, start_date, end_date, 
		datediff(end_date, start_date) as reallocated_days
	from customer_nodes n
	inner join regions r using(region_id)
	where end_date <> '9999-12-31'),
cte2 as (
	select *, row_number() over(partition by region_id order by reallocated_days) as rnk,
		count(*) over(partition by region_id) as region_count
	from cte1
	where reallocated_days <> 0),
cte3 as (
	select *, 
		floor(region_count * 0.8) as 80th_percentile_index,
		floor(region_count * 0.95) as 95th_percentile_index,
		if(region_count % 2 = 0, round(region_count/2,0), round((region_count+1)/2,0)) as median_index
	from cte2),
cte4 as (
	select region_id, region_name, reallocated_days as median_days
	from cte3
	where rnk = median_index),
cte5 as (
	select region_id, region_name, reallocated_days as precentile_80th_days
	from cte3
	where rnk = 80th_percentile_index),
cte6 as (
	select region_id, region_name, reallocated_days as precentile_95th_days
	from cte3
	where rnk = 95th_percentile_index)
select c4.region_name, median_days, precentile_80th_days,
	precentile_95th_days
from cte4 c4
join cte5 c5 using(region_id)
join cte6 c6 using(region_id);

-- B. Customer Transactions
/* 1. What is the unique count and total amount for each transaction type? */
with cte1 as (
	select distinct txn_type, sum(txn_amount) over(partition by txn_type) as total_amt
	from customer_transactions),
cte2 as (
	select count(distinct customer_id) as unique_count, txn_type
	from customer_transactions
	group by 2)
select t1.txn_type as transaction_type, t2.unique_count, t1.total_amt
from cte1 as t1
inner join cte2 as t2 using(txn_type);

/* 2. What is the average total historical deposit counts and amounts for all customers? */
with cte as (
	select customer_id, txn_type, count(*) as deposit_count,
		avg(txn_amount) as deposit_amount
	from customer_transactions
	where txn_type = 'deposit'
    group by 1, 2)
select txn_type, round(avg(deposit_count)) as avg_deposit_count,
	round(avg(deposit_amount)) as avg_deposit_amount
from cte
group by 1;

/* 3. For each month - how many Data Bank customers make more than 1 deposit and either 1
 purchase or 1 withdrawal in a single month? */
 with txntype_details as (
	 select customer_id, month(txn_date) as month,
		ifnull(sum(case when txn_type = 'deposit' then 1 end), 0) as deposit_cnt,
		ifnull(sum(case when txn_type = 'withdrawal' then 1 end), 0) as withdrawal_cnt,
		ifnull(sum(case when txn_type = 'purchase' then 1 end), 0) as purchase_cnt
	 from customer_transactions
	 group by 1,2
	 order by 2)
select month, count(*) as no_of_customers
from txntype_details
where deposit_cnt > 1 and (purchase_cnt = 1 or withdrawal_cnt = 1)
group by 1;
 
/* 4. What is the closing balance for each customer at the end of the month? */
with cte1 as (
	select customer_id, month(txn_date) as month,
		ifnull(sum(case when txn_type = 'deposit' then txn_amount end), 0) as deposit,
        ifnull(sum(case when txn_type = 'purchase' then (-1)*txn_amount end), 0) as purchase,
		ifnull(sum(case when txn_type = 'withdrawal' then (-1)*txn_amount end), 0) as withdrawal
	from customer_transactions
	group by 1,2
	order by 1),
cte2 as (
	select *, (deposit + purchase + withdrawal) as total
	from cte1)
select customer_id, month,
	sum(total) over(partition by customer_id order by customer_id,month rows between unbounded preceding and current row) as balance,
    total as change_in_balance
from cte2;

/* 5. What is the percentage of customers who increase their closing balance by more than 5%? */
create view txn_details as
	(with cte1 as (
		select customer_id, month(txn_date) as month,
			ifnull(sum(case when txn_type = 'deposit' then txn_amount end), 0) as deposit,
			ifnull(sum(case when txn_type = 'withdrawal' then (-1)*txn_amount end), 0) as withdrawal
		from customer_transactions
		group by 1,2
		order by 1),
	cte2 as (
		select *, (deposit + withdrawal) as total
		from cte1)
	select customer_id, month,
	sum(total) over(partition by customer_id order by customer_id,month rows between unbounded preceding and current row) as balance,
    total as change_in_balance
from cte2);
    
with cte1 as (
	select distinct customer_id,
		first_value(balance) over(partition by customer_id order by customer_id) as first_balance,
		last_value(balance) over(partition by customer_id order by customer_id) as last_balance
	from txn_details),
cte2 as (
	select *,
		round((abs(last_balance/first_balance)-1)*100,2) as growth_rate
	from cte1
	where round((abs(last_balance/first_balance)-1)*100,2) >= 5 and last_balance > first_balance)
select round((count(*) * 100)/(select count(distinct customer_id) from customer_transactions),2) as percent_customer
from cte2;

-- C. Data Allocation Challenge
/* To test out a few different hypotheses - the Data Bank team wants to run an experiment 
where different groups of customers would be allocated data using 3 different options:

Option 1: data is allocated based off the amount of money at the end of the previous month
Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days
Option 3: data is updated real-time

For this multi-part challenge question - you have been requested to generate the following 
data elements to help the Data Bank team estimate how much data will need to be provisioned for each option:

running customer balance column that includes the impact each transaction
customer balance at the end of each month
minimum, average and maximum values of the running balance for each customer
Using all of the data available - how much data would have been required for each option on a monthly basis? */
create view running_balance as (
	with part1 as (
		select customer_id, txn_date,
			ifnull((case when txn_type = "deposit" then txn_amount else (-1)*txn_amount end),0) as txn_amount,
			ifnull(lag(txn_amount) over(partition by customer_id order by txn_date asc),0) as prev_amount
		from customer_transactions
		where txn_type <> "purchase"
		order by 1,2)
	select customer_id, txn_date, sum(txn_amount) as txn_amount, sum(prev_amount) as prev_amount
    from part1
    group by 1,2);
    
with cte1 as (
	select customer_id, txn_date, monthname(txn_date) as month, (txn_amount + prev_amount) as running_amt
	from running_balance),
cte2 as (
	select *,
		last_value(running_amt) over (partition by customer_id order by month(txn_date)) as monthend_balance
	from cte1),
cte3 as (    
	select *, min(running_amt) over (partition by customer_id) as min_balance,
		max(running_amt) over (partition by customer_id) as max_balance,
		round(avg(running_amt) over (partition by customer_id),2) as avg_balance,
        rank() over(partition by customer_id order by txn_date desc) as rnk
	from cte2)
select customer_id, txn_date, month, running_amt,
	monthend_balance, min_balance, max_balance, avg_balance
from cte3
where rnk = 1;

-- D. Extra Challenge
/* Data Bank wants to try another option which is a bit more difficult to implement - they want to 
calculate data growth using an interest calculation, just like in a 
traditional savings account you might have with a bank.

If the annual interest rate is set at 6% and the Data Bank team wants to reward its customers 
by increasing their data allocation based off the interest calculated on a daily basis at the end 
of each day, how much data would be required for this option on a monthly basis?

Special notes:

Data Bank wants an initial calculation which does not allow for compounding interest, 
however they may also be interested in a daily compounding interest calculation so you can try 
to perform this calculation if you have the stamina! */
with cte1 as (
	select customer_id,
		txn_date,
		sum(txn_amount) as total_data,
		cast(concat(year(txn_date),"-",month(txn_date),"-",1) as date) as month_start_date
	from customer_transactions
	group by 1, 2
	order by 1),
cte2 as (
	select *, datediff(txn_date,month_start_date) as days,
		(total_data * power((1 + 0.06/365), datediff('1900-01-01', txn_date))) as daily_interest_data
	from cte1)
select customer_id,
	month_start_date as txn_month,
    round(sum(daily_interest_data * days),2) as data_required
from cte2
group by 1,2
order by 1;

-- Extension Request
/* The Data Bank team wants you to use the outputs generated from the above sections to 
create a quick Powerpoint presentation which will be used as marketing materials for both 
external investors who might want to buy Data Bank shares and new prospective customers 
who might want to bank with Data Bank.

Using the outputs generated from the customer node questions, generate a few headline 
insights which Data Bank might use to market it’s world-leading security features to 
potential investors and customers.

With the transaction analysis - prepare a 1 page presentation slide which contains all 
the relevant information about the various options for the data provisioning so the Data 
Bank management team can make an informed decision.*/