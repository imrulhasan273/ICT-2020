
-- get the merchant: Padma MBP
select * from surecash.dba_all_merchant dam where biller_wallet = '017113863298'; --PMBP

-- get all disburement-withdraw data
drop table if exists training.disburse_withdraw_tmp;

create table training.disburse_withdraw_tmp as
select
	bulk_payment_id,
	count(distinct case when sbpt.txncode in(11511, 11) then sbpt.txn_number end) as no_of_withdraw,
	sum(case when sbpt.txncode in(11511, 11) then sbpt.deducted_bulk_amount else 0 end) as withdraw_amount,
	replace (array_agg(distinct case when txncode in(11511) then stl.interbanktoac when sbpt.txncode in(11) then stl.destac end)::varchar,',','/') as agent_wallet,
	replace (array_agg(distinct stl.requesttime::date)::varchar,',','/') as withdraw_date,
	count(distinct case when sbpt.txncode in(1201511, 1201) then sbpt.txn_number end) as no_of_p2p,
	sum(case when sbpt.txncode in(1201511, 1201) then sbpt.deducted_bulk_amount else 0 end) as p2p_amount,
	count(distinct case when sbpt.txncode in(1202511, 1202) then sbpt.txn_number end) as no_of_p2b,
	sum(case when sbpt.txncode in(1202511, 1202) then sbpt.deducted_bulk_amount else 0 end) as p2b_amount,
	count(distinct case when sbpt.txncode in(1602, 16021) then sbpt.txn_number end) as no_of_recharge,
	sum(case when sbpt.txncode in(1602, 16021) then sbpt.deducted_bulk_amount else 0 end) as recharge_amount,
	count(1) as ttv,
	sum(deducted_bulk_amount) as tpv
from
	surecash.sc_bulk_processed_transaction sbpt
left join 
(
	select
		trnxid, requesttime, srcac, destac, interbanktoac
	from
		surecash.sc_transaction_log
	where
		requesttime >= '2020-12-08' 
) stl on stl.trnxid = sbpt.txn_number
where
	create_date >= '2020-12-08'
	--and txncode in(11511,11)
group by 	
	bulk_payment_id;
	

-- join with disburement with withdraw
drop table if exists training.disburse_withdraw_rpt_tmp;

create table training.disburse_withdraw_rpt_tmp as	
select
	sbp.to_ac as wallet,
	coalesce(sbp.disbursed_amount, 0) as disburse_amount,
	coalesce(tmp.no_of_withdraw, 0) as no_of_withdraw,
	coalesce(tmp.withdraw_amount, 0) as withdraw_amount,
	agent_wallet::varchar as agent_wallet,
	withdraw_date::varchar as withdraw_date,
	coalesce(tmp.no_of_p2p, 0) as no_of_p2p,
	coalesce(tmp.p2p_amount, 0) as p2p_amount,
	coalesce(tmp.no_of_p2b, 0) as no_of_p2b,
	coalesce(tmp.p2b_amount, 0) as p2b_amount,
	coalesce(tmp.no_of_recharge, 0) as no_of_recharge,
	coalesce(tmp.recharge_amount, 0) as recharge_amount,
	coalesce(tmp.ttv, 0) as ttv,
	coalesce(tmp.tpv, 0) as tpv
from
	surecash.sc_bulk_payment as sbp
left join training.disburse_withdraw_tmp as tmp on tmp.bulk_payment_id = sbp.id
where
	sbp.create_date >= '2020-12-08'
	and upper(sbp.from_ac) = '017113863298'; ---PADMA MERCHANT [CODE:PMBP]
	

select * from training.disburse_withdraw_tmp;


select * from training.disburse_withdraw_rpt_tmp ;



select sum(disburse_amount) from training.disburse_withdraw_rpt_tmp ;


select sum(withdraw_amount) from training.disburse_withdraw_rpt_tmp ;

--d:6222    |	 w:197293  -> 206873 --> going...>>>.....




