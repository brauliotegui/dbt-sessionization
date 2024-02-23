/*

 Use the ID generated while creating dbt_aliases_mapping to link all events for the same user on that device. Also note the idle time between events

*/

{{ config(materialized='table') }}

select *
        ,DATEDIFF('MINUTE', LAG("TIMESTAMP") OVER (PARTITION BY DBT_VISITOR_ID ORDER BY "TIMESTAMP"), "TIMESTAMP") AS IDLE_TIME_MINUTES
      from (
        select t.id as event_id
          ,t.anonymous_id
          ,a2v.dbt_visitor_id
          ,t.timestamp
          ,t.event as event
        from {{ source('src_rudderstack', 'PUBLIC_TRACKS') }} as t
        inner join {{ ref('dbt_aliases_mapping') }} as a2v
        on a2v.alias = coalesce(t.user_id, t.anonymous_id)
        )
        LEFT JOIN 
        ( SELECT id, org_slug, user_email 
          FROM {{ ref('mrt_user_events')}}
        ) t2
        ON event_id = t2.id
