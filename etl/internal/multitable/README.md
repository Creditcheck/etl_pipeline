Important note about the Engine.Run(...) call above

Your earlier “generic engine” took its own Pipeline struct. To avoid rewriting it here, I used a small adapter (mrAdapter) in the next file to pass already-collected recs. If you’d rather, we can simplify by changing Engine.Run to accept MultiPipeline + []Record directly (cleaner), but I kept changes minimal.
