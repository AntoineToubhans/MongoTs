# Demo

- Add a demo time serie webpage
- a graph (d3js)
- selector for generating data:
  - fake data
  - sample (find ones)
- selector for the connector

# Fixed_range connector

- missing aggregateParams (default value)
- missing groupbyParams (default value)
- missing aggregateParams (ignore)
- missing groupbyParams (ignore)
- custom aggregation (instead of hard-coded count + sum)
- custom aggregation range (years, day, milisecond, whatever)
- perf: improve first insert time => too long to create fully filled document
- add a unique compound index on tag + date

# connectors

- wording : groupbyParams => tags
- add "getAggregate" to connectors
- add "pushMany" to connectors (bulk)
- missing time => infer now()
- interpolation ? instead of aggregation

# Done

- Connector's "push" return a standardized response (nInserted)
- pep8 => snake_case
- fully filled document
