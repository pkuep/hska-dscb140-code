drop database if exists webshop;
create database webshop;
use webshop;

create table status (
statusid int primary key,
bearbeitungsstatus varchar(50)
);


create table kategorie (
katid int primary key,
bezeichnung varchar(50)
);


create table benutzer (
email varchar(100) primary key,
passwort varchar(50) not null,
vorname varchar(50) not null,
nachname varchar(50) not null
);


-- -----------------
create table produkt (
pnr int primary key,
bezeichnung varchar(50),
angelegt_am timestamp,
verfuegbarkeit int not null,
preis decimal(6,2) not null,
katid int not null,
foreign key (katid) references kategorie(katid) on delete cascade
);


-- -----------------

create table abteilung (
abtid int primary key,
bezeichnung varchar(50),
uebergeordnet_abtid int, -- Fremdschlüssel hier nicht NOT NULL! Kardinalitäten der Hierarchie!
foreign key (uebergeordnet_abtid) references abteilung (abtid) on delete cascade  -- Fremdschlüssel-Constraint
);

-- -----------------

create table kunde (
email varchar(100) primary key, -- Primärschlüssel und gleichzeitg Fremdschlüssel
kdnr int unique,  -- Spalte soll eindeutig sein
strasse varchar(50),
hausnummer varchar(10),
plz varchar(5),
ort varchar(50),
geburtsdatum date,
foreign key (email) references benutzer (email) on delete cascade  -- Fremdschlüssel-Constraint
);

create table mitarbeiter (
email varchar(100) primary key, -- Primärschlüssel und gleichzeitg Fremdschlüssel
manr int unique,  -- Spalte soll eindeutig sein
telefonnr varchar(30),
abtid int not null,
foreign key (email) references benutzer (email) on delete cascade,  -- Fremdschlüssel-Constraint email
foreign key (abtid) references abteilung (abtid) on delete cascade  -- Fremdschlüssel-Constraint abtid
);
-- ----------------------------

create table bestellung ( 
bestnr int auto_increment primary key, -- automatische Hochzählen aktivieren
bestelldatum TIMESTAMP,
bearbeitet_email varchar(100) not null,  -- min-Kardinalität 1
foreign key (bearbeitet_email) references mitarbeiter(email) on delete cascade, -- FK-Constraint
kunde varchar(100) not null,  -- min-Kardinalität 1
foreign key (kunde) references kunde(email) on delete cascade, -- FK-Constraint
statusid int not null, -- min-Kardinalität 1
foreign key (statusid) references status(statusid) on delete cascade -- FK-Constraint
);


-- --------------------

create table warenkorbposition (
kunde_email varchar(100), -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
foreign key (kunde_email) references kunde(email) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
pnr int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
foreign key (pnr) references produkt(pnr) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
menge int,
primary key (kunde_email, pnr) -- Definition eines zusammengesetzten Primärschlüssels
);


create table bestellposition (	
bestnr int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
foreign key (bestnr) references bestellung(bestnr) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
pnr int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
foreign key (pnr) references produkt(pnr) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
menge int,
primary key (bestnr, pnr) -- Definition eines zusammengesetzten Primärschlüssels, 8);
);

create table gemeinsamerkauf (
k1_katid int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
foreign key (k1_katid) references kategorie(katid) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
k2_katid int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
foreign key (k2_katid) references kategorie(katid) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
primary key (k1_katid, k2_katid) -- Definition eines zusammengesetzten Primärschlüssel1, 2);
);
