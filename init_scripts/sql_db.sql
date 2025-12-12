BEGIN;


CREATE TABLE IF NOT EXISTS public.rooms
(
    id serial NOT NULL,
    name character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT rooms_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.students
(
    birthday date,
    id serial NOT NULL,
    name character varying(50) COLLATE pg_catalog."default",
    room integer,
    sex character varying(5) COLLATE pg_catalog."default",
    CONSTRAINT students_pkey PRIMARY KEY (id)
);

ALTER TABLE IF EXISTS public.students
    ADD CONSTRAINT students_room_fkey FOREIGN KEY (room)
    REFERENCES public.rooms (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

END;