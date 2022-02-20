--terra1yrc0zpwhuqezfnhdgvvh7vs5svqtgyl7pu3n6c router
--terra1p7jp8vlt57cf8qwazjg58qngwvarmszsamzaru yStaking
--terra19d2alknajcngdezrdhq40h6362k92kz23sz62u PRISM-UST
--terra1czynvm64nslq2xxavzyrrhau09smvana003nrf PRISM-xPRISM
--terra1yxgq5y6mw30xy9mmvz9mllneddy9jaxndrphvk PRISM-cLUNA
--terra1persuahr6f8fm6nyup0xjc7aveaur89nwgs5vs PRISM-pLUNA
--terra1kqc65n5060rtvcgcktsxycdt2a4r67q2zlvhce PRISM-yLUNA
--terra1r38qlqt69lez4nja5h56qwf4drzjpnu8gz04jd PRISM-LUNA
with swaps as (
select block_timestamp, tx_id, event_index, MSG_INDEX,
  	   EVENT_ATTRIBUTES:from from_,
  	   EVENT_ATTRIBUTES:to to_,
  	   replace(EVENT_ATTRIBUTES:offer_amount,'\"','')/1000000 offer_amount,
  	   replace(EVENT_ATTRIBUTES:return_amount,'\"','')/1000000 return_amount,
   	   replace(EVENT_ATTRIBUTES:offer_amount,'\"','')/replace(EVENT_ATTRIBUTES:return_amount,'\"','') price,
  	   replace(replace(replace(EVENT_ATTRIBUTES:ask_asset,'\"',''),'cw20:',''),'native:','') ask_asset,
  	   replace(replace(replace(EVENT_ATTRIBUTES:offer_asset,'\"',''),'cw20:',''),'native:','') offer_asset
from terra.msg_events
where (
  --PRISM-UST
  EVENT_ATTRIBUTES:from='terra19d2alknajcngdezrdhq40h6362k92kz23sz62u' or
  EVENT_ATTRIBUTES:to='terra19d2alknajcngdezrdhq40h6362k92kz23sz62u' or 
  --PRISM-xPRISM
  EVENT_ATTRIBUTES:"0_from"='terra1czynvm64nslq2xxavzyrrhau09smvana003nrf' or
  EVENT_ATTRIBUTES:"0_to"='terra1czynvm64nslq2xxavzyrrhau09smvana003nrf' or
  --PRISM-cLUNA
  EVENT_ATTRIBUTES:from='terra1yxgq5y6mw30xy9mmvz9mllneddy9jaxndrphvk' or
  EVENT_ATTRIBUTES:to='terra1yxgq5y6mw30xy9mmvz9mllneddy9jaxndrphvk' or 
  --PRISM-pLUNA
  EVENT_ATTRIBUTES:"0_from"='terra1persuahr6f8fm6nyup0xjc7aveaur89nwgs5vs' or
  EVENT_ATTRIBUTES:"0_to"='terra1persuahr6f8fm6nyup0xjc7aveaur89nwgs5vs' or
  --PRISM-yLUNA
  EVENT_ATTRIBUTES:"0_from"='terra1kqc65n5060rtvcgcktsxycdt2a4r67q2zlvhce' or
  EVENT_ATTRIBUTES:"0_to"='terra1kqc65n5060rtvcgcktsxycdt2a4r67q2zlvhce' or
  --PRISM-LUNA
  EVENT_ATTRIBUTES:"0_from"='terra1r38qlqt69lez4nja5h56qwf4drzjpnu8gz04jd' or
  EVENT_ATTRIBUTES:"0_to"='terra1r38qlqt69lez4nja5h56qwf4drzjpnu8gz04jd')
and tx_status='SUCCEEDED'
and block_timestamp > '2021-09-10T22:53:33Z'::TIMESTAMP_NTZ
and EVENT_ATTRIBUTES:"0_action" in ('swap','send')
and event_type = 'from_contract'
),
senders as (
  select distinct msg_value:sender sender, tx_id
  from terra.msgs
  where tx_id in (
  	select distinct tx_id
  	from swaps
  )
)

select block_timestamp, tx_id, event_index, MSG_INDEX, sender,
  	   from_,
  	   to_,
  	   offer_amount,
  	   return_amount,
   	   price,
  	   CASE
  		when ask_asset='terra1dh9478k2qvqhqeajhn75a2a7dsnf74y5ukregw' then 'PRISM'
  		when ask_asset='terra1042wzrwg2uk6jqxjm34ysqquyr9esdgm5qyswz' then 'xPRISM'
  		when ask_asset='terra13zaagrrrxj47qjwczsczujlvnnntde7fdt0mau' then 'cLUNA'
  		when ask_asset='terra1tlgelulz9pdkhls6uglfn5lmxarx7f2gxtdzh2' then 'pLUNA'
  		when ask_asset='terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz' then 'yLUNA'
  		when ask_asset='uluna' then 'LUNA'
  		when ask_asset='uusd' then 'UST'
  		else ''
  	   end ask_asset,
  	   CASE
  		when offer_asset='terra1dh9478k2qvqhqeajhn75a2a7dsnf74y5ukregw' then 'PRISM'
  		when offer_asset='terra1042wzrwg2uk6jqxjm34ysqquyr9esdgm5qyswz' then 'xPRISM'
  		when offer_asset='terra13zaagrrrxj47qjwczsczujlvnnntde7fdt0mau' then 'cLUNA'
  		when offer_asset='terra1tlgelulz9pdkhls6uglfn5lmxarx7f2gxtdzh2' then 'pLUNA'
  		when offer_asset='terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz' then 'yLUNA'
  		when offer_asset='uluna' then 'LUNA'
  		when offer_asset='uusd' then 'UST'
  		else ''
  	   end offer_asset
from swaps join senders using(tx_id)