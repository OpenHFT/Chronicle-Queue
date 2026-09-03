# Guidance for agents working on Chronicle Queue

## Time-bounded reviewer notes

`//!` has a deliberate lifecycle that differs from an ordinary `//` comment:

* Keep `//` comments concise and suitable for indefinite retention. They explain only the durable invariant a maintainer still needs after the change is established.
* Use `//!` on every behavioural or compatibility change to an existing release-code file while that change is under review. Build metadata needs a note when it changes what downstream users build against. A documentation-only change may need one when its reason is not apparent from the changed text or surrounding behavioural change; use the corresponding comment syntax, for example `<!-- //! ... -->` in XML.
* Do not add `//!` for import tidies, formatting, or a hunk whose only change is the reviewer note itself.
* A `//!` note may be deliberately verbose. Explain what observable failure or compatibility risk required the change, why the simpler-looking alternative is unsafe, and name the best test that fails without the change. If no test discriminates it, say so and give the specific reason the change must remain; otherwise consider dropping the change.
* After every changed hunk is covered, review large hunks again for independent logical changes. One broad `//!` must not make unrelated lifecycle, ordering, compatibility, or failure-policy decisions appear covered; add a separate `//!` section beside each such decision. Do not duplicate notes when the entire hunk enforces one cohesive invariant.
* Perform the converse review as well: every changed test should be named by a relevant release-code `//!`, or have an explicit review disposition explaining its broader integration or compatibility value. If no rationale claims a test and it adds no distinct evidence, drop it.
* A nearby permanent `//` comment does not replace the `//!` evidence. Both may coexist: the concise comment records the lasting rule, while the reviewer note records the change-specific evidence and consequences.
* Do not remove, shorten to an ordinary comment, or omit a relevant `//!` note during cleanup, simplification, squashing, or restacking. Remove a note when its code is removed, when review establishes that no behavioural or compatibility change needs justification, or when maintainers deliberately retire it after the post-merge review period.
* `//!` notes must survive merge so end users can review them. Their eventual retirement will be selected by age with `git blame`; the retention period is intentionally not fixed yet.

This convention intentionally favours reviewability over short-term comment volume. Do not use `///` for these notes: on JDK 23 and later it is a Markdown documentation comment and can fail `-Xlint:all -Werror` when placed next to Javadoc.
