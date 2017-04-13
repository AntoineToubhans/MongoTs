# Demo

- Add a demo time serie webpage
- a graph (d3js)
- selector for generating data:
  - fake data
  - sample (find ones)
- selector for the connector

# Fixed_range connector

- fully filled document
- missing aggregateParams (default value)
- missing groupbyParams (default value)
- missing aggregateParams (ignore)
- missing groupbyParams (ignore)
- custom aggregation (instead of hard-coded count + sum)
- custom aggregation range (years, day, milisecond, whatever)

# connectors

- add "getAggregate" to connectors
- add "pushMany" to connectors (bulk)
- missing time => infer now()

# Done

- Connector's "push" return a standardized response (nInserted)
