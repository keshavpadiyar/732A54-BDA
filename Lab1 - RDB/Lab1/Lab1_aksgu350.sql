#1)
select * from jbemployee e1;

#2)
select * from jbdept order by name;

#3)
select * from jbparts where   qoh = 0;

#4)
select * from jbemployee where salary between 9000 and 10000 order by salary;

#5)
select Name,birthyear,startyear, startyear-birthyear as age from jbemployee;

#6) 
select * from jbemployee where substring(name, 1, position("," IN Name)-1)like '%son';

#7) 
select * from jbitem where supplier in (select id from jbsupplier where  name = "fisher-price");

#8)
select i.*, s.name from jbitem i inner join jbsupplier s
on i.supplier = s.id
where s.name = "fisher-price";

#9)
select * from jbcity where id in (select distinct city from  jbsupplier);

#10) 
select name, color  from jbparts where weight > 
(select weight from jbparts where name = 'card reader' ) ;

#11) 
select a.name, a.color from jbparts a, jbparts b
where a.weight > b.weight and b.name = 'card reader' ;

#12) 
select color, avg(weight) as avg_weight from jbparts where color = 'black';

#13) 
select s.name,sum(p.weight * sup.quan) total_weight from jbsupplier s 
inner join jbcity c on s.city = c.id inner join jbsupply sup on sup.supplier = s.id inner join jbparts p on sup.part = p.id
where c.state = "mass"
group by s.name;

#14)

DROP TABLE IF EXISTS  jbitems CASCADE;

CREATE TABLE  jbitems (
    id INT,
    name VARCHAR(20),
    dept INT NOT NULL,
    price INT,
    qoh INT UNSIGNED /* or, if check constraints were enforced: INT CHECK (qoh >= 0)*/,
    supplier INT NOT NULL,
    CONSTRAINT pk_item PRIMARY KEY(id)) ENGINE=InnoDB;
    
ALTER TABLE jbitems ADD CONSTRAINT fk_item_deptartment FOREIGN KEY (dept) REFERENCES jbdept(id);

ALTER TABLE jbitems ADD CONSTRAINT fk_item_supplier_new FOREIGN KEY (supplier) REFERENCES jbsupplier(id);

INSERT INTO jbitems
(select * from jbitem where price < (select avg(price) from jbitem));
    
select * from jbitems;