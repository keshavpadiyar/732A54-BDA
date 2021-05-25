#1) List all employees, i.e. all tuples in the jbemployee relation.
select e1.name as Manager,e2.name as Employee, e2.salary,e2.birthyear,e2.startyear
 from jbemployee e1,jbemployee e2 where e2.manager = e1.id;

#2) List the name of all departments in alphabetical order. Note: by “name” we mean the name attribute for all tuples in the jbdept relation.
select * from kespa139.jbdept where id is not null order by name;

#3) What parts are not in store, i.e. qoh = 0? (qoh = Quantity On Hand)
select * from kespa139.jbparts where id is not null and qoh = 0;

#4) Which employees have a salary between 9000 (included) and 10000 (included)?
select * from kespa139.jbemployee where id is not null and  salary between 9000 and 10000 order by salary;

#5) What was the age of each employee when they started working (startyear)?
select *, startyear-birthyear as age from kespa139.jbemployee where id is not null;

#6) Which employees have a last name ending with “son”?
select * from kespa139.jbemployee where substring(name, 1, position("," IN Name)-1)like '%son' and id is not null;

#7) Which items (note items, not parts) have been delivered by a supplier called Fisher-Price? Formulate this query 
#using a subquery in the where-clause.
select * from kespa139.jbitem where supplier in (select id from kespa139.jbsupplier where id is not null and lower(trim(name)) = "fisher-price");

#8) Formulate the same query as above, but without a subquery.
select item.*, supplier.name from kespa139.jbitem item inner join kespa139.jbsupplier supplier 
on item.supplier = supplier.id
where lower(trim(supplier.name)) = "fisher-price";

#9) Show all cities that have suppliers located in them. Formulate this query using a subquery in the where-clause.
select count(*) from kespa139.jbcity where id in (select distinct city from  kespa139.jbsupplier);

#10) What is the name and color of the parts that are heavier than a card reader? Formulate this query using a subquery 
#in the where-clause. (The SQL query must not contain the weight as a constant.)
select name, color  from kespa139.jbparts where weight > 
(select weight from kespa139.jbparts where lower(trim(name)) = 'card reader' ) ;

#11) Formulate the same query as above, but without a subquery. (The query must not contain the weight as a constant.)
select p1.name, p1.color from kespa139.jbparts p1,kespa139.jbparts p2
where p1.weight > p2.weight and lower(trim(p2.name)) = 'card reader' ;

#12) What is the average weight of black parts?
select color, avg(weight) as avg_weight from kespa139.jbparts where color = 'black';

#13) What is the total weight of all parts that each supplier in Massachusetts (“Mass”) has delivered?
# Retrieve the name and the total weight for each of these suppliers. 
#Do not forget to take the quantity of delivered parts into account. Note that one row should be returned for each supplier
select supplier.name,sum(parts.weight * supply.quan) total_weight from kespa139.jbsupplier supplier inner join kespa139.jbcity city
on supplier.city = city.id
inner join kespa139.jbsupply supply
on supply.supplier = supplier.id
inner join kespa139.jbparts parts
on supply.part = parts.id
where lower(trim(city.state)) = "mass"
group by supplier.name;

#14)Create a new relation (a table), with the same attributes as the table items using
#the CREATE TABLE syntax where you define every attribute explicitly (i.e. not
#as a copy of another table). Then fill the table with all items that cost less than the
#average price for items. Remember to define primary and foreign keys in your
#table!

DROP TABLE IF EXISTS  kespa139.jbitems CASCADE;

CREATE TABLE  kespa139.jbitems (
    id INT,
    name VARCHAR(20),
    dept INT NOT NULL,
    price INT,
    qoh INT UNSIGNED /* or, if check constraints were enforced: INT CHECK (qoh >= 0)*/,
    supplier INT NOT NULL,
    CONSTRAINT pk_item PRIMARY KEY(id)) ENGINE=InnoDB;
    
ALTER TABLE kespa139.jbitems ADD CONSTRAINT fk_item_dept_n FOREIGN KEY (dept) REFERENCES kespa139.jbdept(id);

ALTER TABLE kespa139.jbitems ADD CONSTRAINT fk_item_supplier_n FOREIGN KEY (supplier) REFERENCES kespa139.jbsupplier(id);

INSERT INTO kespa139.jbitems
(select * from kespa139.jbitem
	where price < (select avg(price) from kespa139.jbitem));
    
select * from kespa139.jbitems;