/*

We leverage analytic functions like first_value and nth_value to create 5-event sequences that capture the flow of events during a session. 5 can be increased or decreased as per requirements.

*/

{{ config(materialized='table') }}

with derived_table as (
          select
            event_id,
            session_id,
            track_sequence_number,
            FIRST_VALUE(EVENT IGNORE NULLS) OVER (PARTITION BY SESSION_ID ORDER BY TRACK_SEQUENCE_NUMBER ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS EVENT,
            dbt_visitor_id,
            timestamp,
            NTH_VALUE(EVENT, 2) OVER (PARTITION BY SESSION_ID ORDER BY TRACK_SEQUENCE_NUMBER ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS SECOND_EVENT,
            NTH_VALUE(EVENT, 3) OVER (PARTITION BY SESSION_ID ORDER BY TRACK_SEQUENCE_NUMBER ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS THIRD_EVENT,
            NTH_VALUE(EVENT, 4) OVER (PARTITION BY SESSION_ID ORDER BY TRACK_SEQUENCE_NUMBER ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FOURTH_EVENT,
            NTH_VALUE(EVENT, 5) OVER (PARTITION BY SESSION_ID ORDER BY TRACK_SEQUENCE_NUMBER ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FIFTH_EVENT,
            from {{ ref('dbt_track_facts') }}
        )

          select event_id
            , session_id
            , track_sequence_number
            , event
            , dbt_visitor_id
            , cast(timestamp as timestamp) as timestamp
            , second_event as event_2
            , third_event as event_3
            , fourth_event as event_4
            , fifth_event as event_5
      from derived_table a
