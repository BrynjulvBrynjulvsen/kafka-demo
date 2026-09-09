// Keep these small assets in the repository so JVM builds need no Node tooling.
import { copyFile, mkdir } from 'node:fs/promises';
const source = new URL('../node_modules/reveal.js/', import.meta.url);
const destination = new URL('../src/main/resources/static/vendor/reveal/', import.meta.url);
await mkdir(destination, { recursive: true });
for (const file of ['dist/reveal.esm.js', 'dist/reveal.css', 'LICENSE']) {
  await copyFile(new URL(file, source), new URL(file.split('/').at(-1), destination));
}
