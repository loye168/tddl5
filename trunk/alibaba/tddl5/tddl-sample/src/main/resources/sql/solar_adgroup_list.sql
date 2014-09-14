SELECT 
    b.thedate, b. memberid, b.campaignid, b.productlineid, b.adgroupid, 
    sum(b.impression), sum(b.cost), sum(b.click),(sum(b.click)/sum(b.impression)) as ctr, (sum(b.cost) / sum(b.click)) as ppc,
    a.title, a.bid_price, a.online_status,a.audit_status,a.audit_reason,
	c.title,c.start_time,c.end_time,c.online_status,c.settle_status,c.settle_reason,c.type
FROM
    adgroup a left join rpt_solar_adgroup_ob b on a.id=b.adgroupid
    LEFT join campaign c on c.id=a.campaign_id

WHERE
	a.MEMBER_ID=10150498 AND c.MEMBER_ID=10150498
    AND a.title like '%连衣裙%'
    AND a.online_status in (1,2,3)
	   AND c.type=1
    AND b.thedate between '2013-12-12' AND '2013-12-31'
	   AND a.bid_price>10
GROUP BY
    b.thedate, b.memberid, b.campaignid, b.productlineid, b.adgroupid
HAVING
    ppc>1
ORDER BY 
    ctr DESC
LIMIT 
    0,100;
