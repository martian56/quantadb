# QuantaDB design system

Scope: the static marketing site in `web/`. Written 2026-07-25 with the
rebuild of `web/index.html`.

## Direction contract

**Thesis.** The comparison table is the argument. It sits in the first
viewport at full width, carries its own provenance, and shows the workloads
QuantaDB loses as plainly as the ones it wins. It refuses the arrangement
this category ships by default, where a claim headline sits above three
feature cards and the measurements are a link.

**Own world.** Near-black ground, one violet accent, one neutral ramp, and
figures set in mono on a shared baseline. Data bars are structural, not
decorative: every bar is a real ratio against the leader of its own column.
No gradients, no glass, no rounded card stacks.

**Story.** A skeptical engineer sees four engines measured under identical
limits, finds the commit and date attached, sees QuantaDB lose on writes,
and concludes the numbers were not selected for flattery.

**First viewport.** Wordmark and nav, one headline line, one supporting
line, two actions, then the field table edge to edge with its provenance
strip. The primary action sits above the table on desktop and directly
under the headline on mobile.

**Form.** Developer infrastructure convention, played straight, at the craft
level of Neon, Turso and ClickHouse. Chosen by the user as the standing exit
from the direction roll on 2026-07-25.

## Color

Ground `#0B0C0E`. Raised surface `#131519`. Rule `#23262C`. Body text
`#E6E8EC`. Secondary text `#9AA1AC`. Accent `#9B7BFF` for interactive and
data, brand `#7132F5` reserved for the mark. Amber `#E0A458` marks the
engine leading a column when it is not QuantaDB.

Strategy is restrained: neutrals plus one accent, because the visitor came
to read numbers. Dark is chosen from the scene, not the category: this is
read on a laptop beside a terminal.

## Type

System grotesque for prose, system mono for every figure and identifier.
No webfonts, because the site loads no external resources. Hierarchy is
carried by scale and weight, not by family. Display caps at 4rem. Body
measure stays inside 68ch.

## Rules

- Figures always in mono, tabular numerals, right aligned on their column.
- A number never appears without its unit and its provenance reachable in
  one glance or one link.
- Bars share one scale per column and are labelled; a bar is never a
  decoration in a cell that has no ratio to express.
- Sections vary: table, matrix, prose pair, code, list. No repeating rig of
  eyebrow over heading over paragraph.
- One authored motion moment, on the bars, from an already-visible default.
- The benchmark blocks between the `bench:` markers belong to CI. Do not
  hand-edit them.
