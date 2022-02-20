/*
{
   "0_to":"terra1xw3h7jsmxvh6zse74e4099c6gl03fnmxpep76h",
   "1_to":"terra1uh37lsydrup8vqvvttd53qwj93ft9x7572g62q",
   "2_to":"terra1uh37lsydrup8vqvvttd53qwj93ft9x7572g62q",
   "from":"terra1uh37lsydrup8vqvvttd53qwj93ft9x7572g62q",
   "bonded":7976801,
   "minted":7976801,
   "0_action":"bond_split",
   "0_amount":7976801,
   "1_action":"mint",
   "1_amount":7976801,
   "2_action":"mint",
   "2_amount":7976801,
   "3_action":"mint",
   "0_contract_address":"terra1xw3h7jsmxvh6zse74e4099c6gl03fnmxpep76h",
   "1_contract_address":"terra13zaagrrrxj47qjwczsczujlvnnntde7fdt0mau",
   "2_contract_address":"terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz",
   "3_contract_address":"terra1tlgelulz9pdkhls6uglfn5lmxarx7f2gxtdzh2"
}
B3C6C3B08325F9FD9FD65DD35CA2CDA1AEF337DEE328272F2B74C970C50457EB
{
   "to":"terra1u02kzrt3glv6060n9al46l4x4w92628gcn7tcv",
   "0_by":"terra1xw3h7jsmxvh6zse74e4099c6gl03fnmxpep76h",
   "1_by":"terra1xw3h7jsmxvh6zse74e4099c6gl03fnmxpep76h",
   "0_from":"terra1u02kzrt3glv6060n9al46l4x4w92628gcn7tcv",
   "1_from":"terra1u02kzrt3glv6060n9al46l4x4w92628gcn7tcv",
   "2_from":"terra1xw3h7jsmxvh6zse74e4099c6gl03fnmxpep76h",
   "0_action":"burn_from",
   "0_amount":17970589,
   "1_action":"burn_from",
   "1_amount":17970589,
   "2_action":"transfer",
   "2_amount":17970589,
   "0_contract_address":"terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz",
   "1_contract_address":"terra1tlgelulz9pdkhls6uglfn5lmxarx7f2gxtdzh2",
   "2_contract_address":"terra13zaagrrrxj47qjwczsczujlvnnntde7fdt0mau"
}
*/

with refract as (
select block_timestamp, tx_id, event_index, MSG_INDEX,
  	   coalesce(EVENT_ATTRIBUTES:from,EVENT_ATTRIBUTES:"0_from") from_,
  	   coalesce(EVENT_ATTRIBUTES:to,EVENT_ATTRIBUTES:"0_to") to_,
  	   replace(EVENT_ATTRIBUTES:"0_action",'\"','') "0_action",
  	   replace(EVENT_ATTRIBUTES:"1_contract_address",'\"','') "1_contract_address",
  	   replace(EVENT_ATTRIBUTES:"2_contract_address",'\"','') "2_contract_address",
  	   replace(EVENT_ATTRIBUTES:"3_contract_address",'\"','') "3_contract_address",
  	   replace(EVENT_ATTRIBUTES:"0_amount",'\"','')/1000000 "0_amount",
  	   replace(EVENT_ATTRIBUTES:"1_amount",'\"','')/1000000 "1_amount",
  	   replace(EVENT_ATTRIBUTES:"2_amount",'\"','')/1000000 "2_amount",
  	   replace(EVENT_ATTRIBUTES:"bonded",'\"','')/1000000 "bonded",
  	   replace(EVENT_ATTRIBUTES:"minted",'\"','')/1000000 "minted"
from terra.msg_events
where (
  --refraction
  EVENT_ATTRIBUTES:"0_to"='terra1xw3h7jsmxvh6zse74e4099c6gl03fnmxpep76h')
and tx_status='SUCCEEDED'
and block_timestamp > '2021-09-10T22:53:33Z'::TIMESTAMP_NTZ
and (EVENT_ATTRIBUTES:"0_action" in ('bond_split','burn_from'))
and event_type = 'from_contract'
)

select *
from refract
