-- Every fired trigger event has exactly one decision, and only fired events have one.
{{ config(tags=['decisioning']) }}
select e.trigger_event_id, e.status, count(d.decision_id) as decisions
from {{ ref('fct_trigger_event') }} e
left join {{ ref('fct_offer_decision') }} d on d.trigger_event_id = e.trigger_event_id
group by e.trigger_event_id, e.status
having (e.status = 'FIRED' and count(d.decision_id) <> 1)
    or (e.status <> 'FIRED' and count(d.decision_id) <> 0)
