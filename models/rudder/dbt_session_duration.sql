/*

Table containing a useful session metric - session duration

*/

{{ config(materialized='view') }}

select 
    s1.dbt_visitor_id
    , s1.session_id
    , DATEDIFF('MINUTE', CAST(S1.SESSION_START_AT AS TIMESTAMP), CAST(S2.ENDED_AT AS TIMESTAMP)) as SESSION_DURATION
from
    {{ ref('dbt_session_tracks')}} as s1
    LEFT JOIN {{ ref('dbt_session_track_facts') }} as s2
      ON s1.session_id = s2.session_id
      
    
