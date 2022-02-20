/*
{
   "to":"terra1p7jp8vlt57cf8qwazjg58qngwvarmszsamzaru",
   "from":"terra1m7qh9zcnz06p5rnhad8rzwh4ppq97t96kc4xnm",
   "0_action":"send",
   "0_amount":5000000,
   "1_action":"bond",
   "1_amount":5000000,
   "staker_addr":"terra1m7qh9zcnz06p5rnhad8rzwh4ppq97t96kc4xnm",
   "0_contract_address":"terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz",
   "1_contract_address":"terra1p7jp8vlt57cf8qwazjg58qngwvarmszsamzaru"
}
*/

with staking as (
select block_timestamp, tx_id, event_index, MSG_INDEX,
  	   EVENT_ATTRIBUTES:from from_,
  	   EVENT_ATTRIBUTES:to to_,
  	   replace(EVENT_ATTRIBUTES:"0_action",'\"','') "0_action",
  	   replace(EVENT_ATTRIBUTES:"1_action",'\"','') "1_action",
  	   replace(EVENT_ATTRIBUTES:"1_amount",'\"','')/1000000 "1_amount",
  	   replace(EVENT_ATTRIBUTES:"1_amount",'\"','')/1000000 "0_amount"
from terra.msg_events
where (
  --yLUNA staking
  EVENT_ATTRIBUTES:from='terra1p7jp8vlt57cf8qwazjg58qngwvarmszsamzaru' or
  EVENT_ATTRIBUTES:to='terra1p7jp8vlt57cf8qwazjg58qngwvarmszsamzaru')
and tx_status='SUCCEEDED'
and block_timestamp > '2021-09-10T22:53:33Z'::TIMESTAMP_NTZ
and (EVENT_ATTRIBUTES:"1_action" in ('bond') or EVENT_ATTRIBUTES:"0_action" in ('unbond'))
and event_type = 'from_contract'
)

select *
from staking
