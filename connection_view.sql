select pre_pt_root_id, post_pt_root_id,
 count(*) as n_syn, sum(size) as sum_size,
 stddev(size) as std_size,
ST_MakePoint(stddev(ST_X(ctr_pt_position)),
stddev(ST_Y(ctr_pt_position)) ,
stddev(ST_Z(ctr_pt_position))) AS std_position
FROM synapses_pni_2__minnie3_v1_merge
WHERE pre_pt_root_id!=post_pt_root_id AND
GROUP BY pre_pt_root_id, post_pt_root_id
ORDER BY sum_size DESC