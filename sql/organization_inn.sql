CREATE TABLE data.organization_inn
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    id_organization integer,
    inn character varying COLLATE pg_catalog."default",
    date_add timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT organization_inn_pkey PRIMARY KEY (id),
    CONSTRAINT organization_inn_id_organization_fkey FOREIGN KEY (id_organization)
        REFERENCES data.organization (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE data.organization_inn
    OWNER to dba;