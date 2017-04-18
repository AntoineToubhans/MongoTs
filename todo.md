# Demo

- Add a demo time serie webpage
- a graph (d3js)
- selector for generating data:
  - fake data
  - sample (find ones)
- selector for the connector

# Fixed_range connector

- missing value (default value)
- missing tag (default value)
- missing value (ignore)
- missing tag (ignore)
- custom aggregation (instead of hard-coded count + sum)
- custom aggregation range (years, day, milisecond, whatever)
- perf: improve first insert time => too long to create fully filled document
- add a unique compound index on tag + date

# connectors

- add "getAggregate" to connectors
- add logging
- add "pushMany" to connectors (bulk)
- missing time => infer now()
- interpolation ? instead of aggregation

# Done

- wording : tag_keys => tag_keys
- Connector's "push" return a standardized response (nInserted)
- pep8 => snake_case
- fully filled document
