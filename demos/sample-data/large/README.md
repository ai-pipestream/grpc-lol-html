# Large fixtures

Real pages worth megabytes. **Gitignored**: too big to commit, and they go
stale. Anything `*.html` dropped here appears in the web viewer's dropdown and
can be handed to the bench with `--file`.

The one used in the docs is the WHATWG HTML specification, which is about
15 MiB of real, messy, machine-generated HTML and is thematically apt:

```bash
curl -sL --compressed -o html-spec.html https://html.spec.whatwg.org/
```

Then either watch it:

```bash
cd ../../node-client && npm start      # pick large/html-spec.html
```

or measure it:

```bash
cd ../../../bench
cargo run --release -- --file ../demos/sample-data/large/html-spec.html
```

For reference, that document gives 66,950 matches for the bench's default
selectors, and 415,212 events in the viewer for `a[href], h2, code, dfn`.
