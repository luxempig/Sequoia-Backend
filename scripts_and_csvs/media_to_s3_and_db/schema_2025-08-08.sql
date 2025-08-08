--
-- PostgreSQL database dump
--

-- Dumped from database version 17.4
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS '';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: beverages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.beverages (
    beverage_id integer NOT NULL,
    name text NOT NULL,
    beverage_type text,
    producer text,
    vintage integer,
    region text,
    notes text
);


--
-- Name: beverages_beverage_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.beverages_beverage_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: beverages_beverage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.beverages_beverage_id_seq OWNED BY public.beverages.beverage_id;


--
-- Name: crew_assignments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.crew_assignments (
    voyage_id integer NOT NULL,
    person_id integer NOT NULL,
    role text NOT NULL,
    rank_title text,
    notes text
);


--
-- Name: entity_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entity_tags (
    tag_id integer NOT NULL,
    entity_type text NOT NULL,
    entity_id integer NOT NULL
);


--
-- Name: events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.events (
    event_id integer NOT NULL,
    voyage_id integer,
    title text NOT NULL,
    description text,
    occurred_at timestamp with time zone,
    location text,
    event_type text
);


--
-- Name: events_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.events_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.events_event_id_seq OWNED BY public.events.event_id;


--
-- Name: maintenance_components; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.maintenance_components (
    maintenance_id integer NOT NULL,
    component_id integer NOT NULL,
    action text,
    notes text
);


--
-- Name: maintenance_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.maintenance_events (
    maintenance_id integer NOT NULL,
    vessel_id integer NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    category text,
    description text,
    yard_name text,
    vendor_id integer,
    cost_usd numeric
);


--
-- Name: maintenance_events_maintenance_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.maintenance_events_maintenance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: maintenance_events_maintenance_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.maintenance_events_maintenance_id_seq OWNED BY public.maintenance_events.maintenance_id;


--
-- Name: menu_beverages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.menu_beverages (
    menu_id integer NOT NULL,
    beverage_id integer NOT NULL,
    notes text
);


--
-- Name: menu_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.menu_items (
    menu_item_id integer NOT NULL,
    menu_id integer NOT NULL,
    course text,
    item_name text NOT NULL,
    notes text
);


--
-- Name: menu_items_menu_item_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.menu_items_menu_item_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: menu_items_menu_item_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.menu_items_menu_item_id_seq OWNED BY public.menu_items.menu_item_id;


--
-- Name: menus; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.menus (
    menu_id integer NOT NULL,
    voyage_id integer NOT NULL,
    served_at timestamp with time zone,
    meal_type text,
    notes text
);


--
-- Name: menus_menu_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.menus_menu_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: menus_menu_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.menus_menu_id_seq OWNED BY public.menus.menu_id;


--
-- Name: passengers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.passengers (
    passenger_id integer NOT NULL,
    name text NOT NULL,
    bio_path text,
    basic_info text,
    person_id integer
);


--
-- Name: passengers_passenger_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.passengers_passenger_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: passengers_passenger_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.passengers_passenger_id_seq OWNED BY public.passengers.passenger_id;


--
-- Name: people; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.people (
    person_id integer NOT NULL,
    full_name text NOT NULL,
    alt_names text[],
    birth_date date,
    death_date date,
    bio_path text,
    summary text,
    wiki_url text
);


--
-- Name: people_person_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.people_person_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: people_person_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.people_person_id_seq OWNED BY public.people.person_id;


--
-- Name: ports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ports (
    port_id integer NOT NULL,
    name text NOT NULL,
    region text,
    country text,
    latitude double precision,
    longitude double precision
);


--
-- Name: ports_port_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ports_port_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ports_port_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ports_port_id_seq OWNED BY public.ports.port_id;


--
-- Name: presidents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.presidents (
    president_id integer NOT NULL,
    full_name text NOT NULL,
    term_start date NOT NULL,
    term_end date,
    party text,
    notes text
);


--
-- Name: presidents_president_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.presidents_president_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: presidents_president_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.presidents_president_id_seq OWNED BY public.presidents.president_id;


--
-- Name: source_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_links (
    id integer NOT NULL,
    source_id integer NOT NULL,
    entity_type text NOT NULL,
    entity_id integer NOT NULL,
    context text,
    page_num integer
);


--
-- Name: source_links_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.source_links_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: source_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.source_links_id_seq OWNED BY public.source_links.id;


--
-- Name: source_people; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_people (
    source_id integer NOT NULL,
    person_id integer NOT NULL,
    role text DEFAULT 'Contributor'::text NOT NULL
);


--
-- Name: sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sources (
    source_id integer NOT NULL,
    source_type text NOT NULL,
    source_origin text,
    source_description text,
    source_path text,
    publication_date date,
    publication text,
    headline text,
    page text,
    permalink text,
    archive_ref text,
    sha256 text
);


--
-- Name: sources_source_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sources_source_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sources_source_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sources_source_id_seq OWNED BY public.sources.source_id;


--
-- Name: tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tags (
    tag_id integer NOT NULL,
    name text NOT NULL,
    kind text
);


--
-- Name: tags_tag_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.tags_tag_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tags_tag_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.tags_tag_id_seq OWNED BY public.tags.tag_id;


--
-- Name: vendors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vendors (
    vendor_id integer NOT NULL,
    name text NOT NULL,
    contact text,
    notes text
);


--
-- Name: vendors_vendor_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.vendors_vendor_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: vendors_vendor_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.vendors_vendor_id_seq OWNED BY public.vendors.vendor_id;


--
-- Name: vessel_components; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vessel_components (
    component_id integer NOT NULL,
    vessel_id integer NOT NULL,
    parent_component_id integer,
    name text NOT NULL,
    component_type text,
    serial_number text,
    installed_on date,
    removed_on date,
    notes text
);


--
-- Name: vessel_components_component_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.vessel_components_component_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: vessel_components_component_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.vessel_components_component_id_seq OWNED BY public.vessel_components.component_id;


--
-- Name: vessels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vessels (
    vessel_id integer NOT NULL,
    name text NOT NULL,
    official_no text,
    build_year integer,
    builder text,
    length_ft numeric,
    beam_ft numeric,
    draft_ft numeric,
    material text,
    propulsion text,
    notes text
);


--
-- Name: vessels_vessel_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.vessels_vessel_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: vessels_vessel_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.vessels_vessel_id_seq OWNED BY public.vessels.vessel_id;


--
-- Name: voyage_passengers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.voyage_passengers (
    voyage_id integer NOT NULL,
    passenger_id integer NOT NULL,
    role_on_voyage text,
    boarded_at timestamp with time zone,
    disembarked_at timestamp with time zone,
    notes text
);


--
-- Name: voyage_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.voyage_sources (
    voyage_id integer NOT NULL,
    source_id integer NOT NULL,
    page_num integer
);


--
-- Name: voyage_stops; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.voyage_stops (
    voyage_id integer NOT NULL,
    stop_order integer NOT NULL,
    port_id integer,
    location_name text,
    arrived_at timestamp with time zone,
    departed_at timestamp with time zone,
    notes text
);


--
-- Name: voyages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.voyages (
    voyage_id integer NOT NULL,
    start_timestamp timestamp with time zone NOT NULL,
    end_timestamp timestamp with time zone,
    additional_info text,
    notes text,
    significant_voyage boolean DEFAULT false NOT NULL,
    royalty boolean DEFAULT false NOT NULL,
    owner_label text,
    vessel_id integer
);


--
-- Name: voyage_with_presidency; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.voyage_with_presidency AS
 SELECT v.voyage_id,
    v.start_timestamp,
    v.end_timestamp,
    v.additional_info,
    v.notes,
    (v.significant_voyage)::integer AS "significant_voyage?",
    (v.royalty)::integer AS "royalty?",
    p.president_id,
    p.full_name AS president_name
   FROM (public.voyages v
     LEFT JOIN public.presidents p ON ((((v.start_timestamp)::date >= p.term_start) AND ((p.term_end IS NULL) OR ((v.start_timestamp)::date <= p.term_end)))));


--
-- Name: voyages_voyage_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.voyages_voyage_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: voyages_voyage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.voyages_voyage_id_seq OWNED BY public.voyages.voyage_id;


--
-- Name: beverages beverage_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.beverages ALTER COLUMN beverage_id SET DEFAULT nextval('public.beverages_beverage_id_seq'::regclass);


--
-- Name: events event_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.events ALTER COLUMN event_id SET DEFAULT nextval('public.events_event_id_seq'::regclass);


--
-- Name: maintenance_events maintenance_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.maintenance_events ALTER COLUMN maintenance_id SET DEFAULT nextval('public.maintenance_events_maintenance_id_seq'::regclass);


--
-- Name: menu_items menu_item_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menu_items ALTER COLUMN menu_item_id SET DEFAULT nextval('public.menu_items_menu_item_id_seq'::regclass);


--
-- Name: menus menu_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menus ALTER COLUMN menu_id SET DEFAULT nextval('public.menus_menu_id_seq'::regclass);


--
-- Name: passengers passenger_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.passengers ALTER COLUMN passenger_id SET DEFAULT nextval('public.passengers_passenger_id_seq'::regclass);


--
-- Name: people person_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.people ALTER COLUMN person_id SET DEFAULT nextval('public.people_person_id_seq'::regclass);


--
-- Name: ports port_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ports ALTER COLUMN port_id SET DEFAULT nextval('public.ports_port_id_seq'::regclass);


--
-- Name: presidents president_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.presidents ALTER COLUMN president_id SET DEFAULT nextval('public.presidents_president_id_seq'::regclass);


--
-- Name: source_links id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_links ALTER COLUMN id SET DEFAULT nextval('public.source_links_id_seq'::regclass);


--
-- Name: sources source_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sources ALTER COLUMN source_id SET DEFAULT nextval('public.sources_source_id_seq'::regclass);


--
-- Name: tags tag_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags ALTER COLUMN tag_id SET DEFAULT nextval('public.tags_tag_id_seq'::regclass);


--
-- Name: vendors vendor_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vendors ALTER COLUMN vendor_id SET DEFAULT nextval('public.vendors_vendor_id_seq'::regclass);


--
-- Name: vessel_components component_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vessel_components ALTER COLUMN component_id SET DEFAULT nextval('public.vessel_components_component_id_seq'::regclass);


--
-- Name: vessels vessel_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vessels ALTER COLUMN vessel_id SET DEFAULT nextval('public.vessels_vessel_id_seq'::regclass);


--
-- Name: voyages voyage_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyages ALTER COLUMN voyage_id SET DEFAULT nextval('public.voyages_voyage_id_seq'::regclass);


--
-- Name: beverages beverages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.beverages
    ADD CONSTRAINT beverages_pkey PRIMARY KEY (beverage_id);


--
-- Name: crew_assignments crew_assignments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crew_assignments
    ADD CONSTRAINT crew_assignments_pkey PRIMARY KEY (voyage_id, person_id, role);


--
-- Name: entity_tags entity_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_tags
    ADD CONSTRAINT entity_tags_pkey PRIMARY KEY (tag_id, entity_type, entity_id);


--
-- Name: events events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (event_id);


--
-- Name: maintenance_components maintenance_components_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.maintenance_components
    ADD CONSTRAINT maintenance_components_pkey PRIMARY KEY (maintenance_id, component_id);


--
-- Name: maintenance_events maintenance_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.maintenance_events
    ADD CONSTRAINT maintenance_events_pkey PRIMARY KEY (maintenance_id);


--
-- Name: menu_beverages menu_beverages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menu_beverages
    ADD CONSTRAINT menu_beverages_pkey PRIMARY KEY (menu_id, beverage_id);


--
-- Name: menu_items menu_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menu_items
    ADD CONSTRAINT menu_items_pkey PRIMARY KEY (menu_item_id);


--
-- Name: menus menus_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menus
    ADD CONSTRAINT menus_pkey PRIMARY KEY (menu_id);


--
-- Name: passengers passengers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.passengers
    ADD CONSTRAINT passengers_pkey PRIMARY KEY (passenger_id);


--
-- Name: people people_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.people
    ADD CONSTRAINT people_pkey PRIMARY KEY (person_id);


--
-- Name: ports ports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ports
    ADD CONSTRAINT ports_pkey PRIMARY KEY (port_id);


--
-- Name: presidents presidents_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.presidents
    ADD CONSTRAINT presidents_pkey PRIMARY KEY (president_id);


--
-- Name: source_links source_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_links
    ADD CONSTRAINT source_links_pkey PRIMARY KEY (id);


--
-- Name: source_people source_people_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_people
    ADD CONSTRAINT source_people_pkey PRIMARY KEY (source_id, person_id, role);


--
-- Name: sources sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (source_id);


--
-- Name: sources sources_sha256_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sources
    ADD CONSTRAINT sources_sha256_key UNIQUE (sha256);


--
-- Name: tags tags_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_name_key UNIQUE (name);


--
-- Name: tags tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (tag_id);


--
-- Name: vendors vendors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vendors
    ADD CONSTRAINT vendors_pkey PRIMARY KEY (vendor_id);


--
-- Name: vessel_components vessel_components_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vessel_components
    ADD CONSTRAINT vessel_components_pkey PRIMARY KEY (component_id);


--
-- Name: vessels vessels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vessels
    ADD CONSTRAINT vessels_pkey PRIMARY KEY (vessel_id);


--
-- Name: voyage_passengers voyage_passengers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_passengers
    ADD CONSTRAINT voyage_passengers_pkey PRIMARY KEY (voyage_id, passenger_id);


--
-- Name: voyage_sources voyage_sources_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_sources
    ADD CONSTRAINT voyage_sources_pk PRIMARY KEY (voyage_id, source_id);


--
-- Name: voyage_stops voyage_stops_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_stops
    ADD CONSTRAINT voyage_stops_pkey PRIMARY KEY (voyage_id, stop_order);


--
-- Name: voyages voyages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyages
    ADD CONSTRAINT voyages_pkey PRIMARY KEY (voyage_id);


--
-- Name: idx_presidents_term; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_presidents_term ON public.presidents USING btree (term_start, term_end);


--
-- Name: idx_voyages_end; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_voyages_end ON public.voyages USING btree (end_timestamp);


--
-- Name: idx_voyages_roy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_voyages_roy ON public.voyages USING btree (royalty);


--
-- Name: idx_voyages_sig; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_voyages_sig ON public.voyages USING btree (significant_voyage);


--
-- Name: idx_voyages_start; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_voyages_start ON public.voyages USING btree (start_timestamp);


--
-- Name: idx_vwp_roy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vwp_roy ON public.voyages USING btree (royalty);


--
-- Name: idx_vwp_sig; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vwp_sig ON public.voyages USING btree (significant_voyage);


--
-- Name: idx_vwp_start; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vwp_start ON public.voyages USING btree (start_timestamp);


--
-- Name: uq_source_links_paged; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_source_links_paged ON public.source_links USING btree (source_id, entity_type, entity_id, page_num) WHERE (page_num IS NOT NULL);


--
-- Name: uq_source_links_unpaged; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_source_links_unpaged ON public.source_links USING btree (source_id, entity_type, entity_id) WHERE (page_num IS NULL);


--
-- Name: uq_voyage_sources_paged; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_voyage_sources_paged ON public.voyage_sources USING btree (voyage_id, source_id, page_num) WHERE (page_num IS NOT NULL);


--
-- Name: uq_voyage_sources_unpaged; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_voyage_sources_unpaged ON public.voyage_sources USING btree (voyage_id, source_id) WHERE (page_num IS NULL);


--
-- Name: voyage_sources_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX voyage_sources_unique ON public.voyage_sources USING btree (voyage_id, source_id);


--
-- Name: crew_assignments crew_assignments_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crew_assignments
    ADD CONSTRAINT crew_assignments_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.people(person_id) ON DELETE CASCADE;


--
-- Name: crew_assignments crew_assignments_voyage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crew_assignments
    ADD CONSTRAINT crew_assignments_voyage_id_fkey FOREIGN KEY (voyage_id) REFERENCES public.voyages(voyage_id) ON DELETE CASCADE;


--
-- Name: entity_tags entity_tags_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_tags
    ADD CONSTRAINT entity_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES public.tags(tag_id) ON DELETE CASCADE;


--
-- Name: events events_voyage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_voyage_id_fkey FOREIGN KEY (voyage_id) REFERENCES public.voyages(voyage_id) ON DELETE CASCADE;


--
-- Name: maintenance_components maintenance_components_component_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.maintenance_components
    ADD CONSTRAINT maintenance_components_component_id_fkey FOREIGN KEY (component_id) REFERENCES public.vessel_components(component_id) ON DELETE CASCADE;


--
-- Name: maintenance_components maintenance_components_maintenance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.maintenance_components
    ADD CONSTRAINT maintenance_components_maintenance_id_fkey FOREIGN KEY (maintenance_id) REFERENCES public.maintenance_events(maintenance_id) ON DELETE CASCADE;


--
-- Name: maintenance_events maintenance_events_vendor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.maintenance_events
    ADD CONSTRAINT maintenance_events_vendor_id_fkey FOREIGN KEY (vendor_id) REFERENCES public.vendors(vendor_id);


--
-- Name: maintenance_events maintenance_events_vessel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.maintenance_events
    ADD CONSTRAINT maintenance_events_vessel_id_fkey FOREIGN KEY (vessel_id) REFERENCES public.vessels(vessel_id) ON DELETE CASCADE;


--
-- Name: menu_beverages menu_beverages_beverage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menu_beverages
    ADD CONSTRAINT menu_beverages_beverage_id_fkey FOREIGN KEY (beverage_id) REFERENCES public.beverages(beverage_id) ON DELETE CASCADE;


--
-- Name: menu_beverages menu_beverages_menu_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menu_beverages
    ADD CONSTRAINT menu_beverages_menu_id_fkey FOREIGN KEY (menu_id) REFERENCES public.menus(menu_id) ON DELETE CASCADE;


--
-- Name: menu_items menu_items_menu_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menu_items
    ADD CONSTRAINT menu_items_menu_id_fkey FOREIGN KEY (menu_id) REFERENCES public.menus(menu_id) ON DELETE CASCADE;


--
-- Name: menus menus_voyage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.menus
    ADD CONSTRAINT menus_voyage_id_fkey FOREIGN KEY (voyage_id) REFERENCES public.voyages(voyage_id) ON DELETE CASCADE;


--
-- Name: passengers passengers_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.passengers
    ADD CONSTRAINT passengers_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.people(person_id);


--
-- Name: source_links source_links_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_links
    ADD CONSTRAINT source_links_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(source_id) ON DELETE CASCADE;


--
-- Name: source_people source_people_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_people
    ADD CONSTRAINT source_people_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.people(person_id) ON DELETE CASCADE;


--
-- Name: source_people source_people_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_people
    ADD CONSTRAINT source_people_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(source_id) ON DELETE CASCADE;


--
-- Name: vessel_components vessel_components_parent_component_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vessel_components
    ADD CONSTRAINT vessel_components_parent_component_id_fkey FOREIGN KEY (parent_component_id) REFERENCES public.vessel_components(component_id);


--
-- Name: vessel_components vessel_components_vessel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vessel_components
    ADD CONSTRAINT vessel_components_vessel_id_fkey FOREIGN KEY (vessel_id) REFERENCES public.vessels(vessel_id) ON DELETE CASCADE;


--
-- Name: voyage_passengers voyage_passengers_passenger_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_passengers
    ADD CONSTRAINT voyage_passengers_passenger_id_fkey FOREIGN KEY (passenger_id) REFERENCES public.passengers(passenger_id) ON DELETE CASCADE;


--
-- Name: voyage_passengers voyage_passengers_voyage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_passengers
    ADD CONSTRAINT voyage_passengers_voyage_id_fkey FOREIGN KEY (voyage_id) REFERENCES public.voyages(voyage_id) ON DELETE CASCADE;


--
-- Name: voyage_sources voyage_sources_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_sources
    ADD CONSTRAINT voyage_sources_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(source_id) ON DELETE CASCADE;


--
-- Name: voyage_sources voyage_sources_voyage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_sources
    ADD CONSTRAINT voyage_sources_voyage_id_fkey FOREIGN KEY (voyage_id) REFERENCES public.voyages(voyage_id) ON DELETE CASCADE;


--
-- Name: voyage_stops voyage_stops_port_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_stops
    ADD CONSTRAINT voyage_stops_port_id_fkey FOREIGN KEY (port_id) REFERENCES public.ports(port_id);


--
-- Name: voyage_stops voyage_stops_voyage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyage_stops
    ADD CONSTRAINT voyage_stops_voyage_id_fkey FOREIGN KEY (voyage_id) REFERENCES public.voyages(voyage_id) ON DELETE CASCADE;


--
-- Name: voyages voyages_vessel_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyages
    ADD CONSTRAINT voyages_vessel_fk FOREIGN KEY (vessel_id) REFERENCES public.vessels(vessel_id);


--
-- Name: voyages voyages_vessel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.voyages
    ADD CONSTRAINT voyages_vessel_id_fkey FOREIGN KEY (vessel_id) REFERENCES public.vessels(vessel_id);


--
-- PostgreSQL database dump complete
--

