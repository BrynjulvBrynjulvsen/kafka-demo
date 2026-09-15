export function mountConcepts(roots, registry, nameOf = root => root.dataset.concept) {
  return [...roots].map(root => {
    const name = nameOf(root);
    if (!registry[name]) throw new Error(`Unknown concept: ${name}`);
    return registry[name](root);
  });
}
export function dispatchConcepts(concepts, method, ...args) {
  concepts.forEach(concept => {
    try { concept[method]?.(...args); }
    catch (error) { console.error(`Concept ${method} failed`, error); }
  });
}
export async function initializeDeck(options = {}) {
  const { default: Reveal } = await import('../vendor/reveal/reveal.esm.js');
  const deck = new Reveal({ width: 1200, height: 700, margin: 0.12, hash: true,
    transition: 'fade', controls: true, progress: true, center: true,
    keyboardCondition: event => !event.target.closest('input, textarea, select, button, summary'), ...options });
  await deck.initialize();
  return deck;
}
