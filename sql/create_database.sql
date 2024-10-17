--setup database
drop DATABASE IF EXISTS demo;
create DATABASE demo;
\c demo;

drop table if exists public.result;

create table if not exists public.result
(
    id                VARCHAR(100)     NOT NULL,
    calculated_value  DOUBLE PRECISION NOT NULL,
    write_side_offset BIGINT           NOT NULL,
    PRIMARY KEY (id)
);

-- INSERT INTO public.result
-- VALUES ('1d22e4ec-894c-4304-b2d4-10a691751d88', 0, 1);


