SELECT  
a.custId, a.id,b.adgroupid, SUM(c.cost) / SUM(c.click) AS ppc, 
c.thedate, c.memberid, c.campaignid, c.productlineid, c.adgroupid,  
SUM(c.impression), SUM(c.cost), SUM(c.click), SUM(c.click) / SUM(c.impression) AS ctr,  
b.title, a.onlinestate, a.reason  
FROM Lunaadgroup a join lunaadgroupinfo b on a.id=b.adgroupid   
LEFT JOIN rpt_solar_adgroup_ob c ON a.id = c.adgroupid  
WHERE a.custid='1102000884' and b.custid='1102000884'  and B.TITLE like '鐑攢1s'  and c.click>1000  
GROUP BY c.thedate, c.memberid, c.campaignid, c.productlineid, c.adgroupid  
HAVING ppc > 1  
ORDER BY ppc DESC