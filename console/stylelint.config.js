const token = (group) => new RegExp(`^var\\(--${group}-[a-z0-9-]+\\)$`)
const space = '(0|auto|var\\(--space-[0-9]+\\))'

export default {
  extends: ['stylelint-config-standard'],
  plugins: ['./lint/prefix.js'],
  rules: {
    'picomq/class-prefix': true,
    'declaration-no-important': true,
    'color-no-hex': true,
    'color-named': 'never',
    'function-disallowed-list': ['rgb', 'rgba', 'hsl', 'hsla', 'hwb', 'lab', 'lch', 'oklab', 'oklch', 'color-mix'],
    'property-disallowed-list': ['border-radius', 'box-shadow', 'text-shadow', '/^--/'],
    'declaration-property-value-allowed-list': {
      '/^(color|background|background-color|border-color|outline-color|fill|stroke|caret-color|accent-color)$/': [
        token('color'),
        'none',
        'transparent',
        'currentcolor',
        'inherit',
      ],
      'font-family': [token('font'), 'inherit'],
      'font-size': [token('text'), 'inherit'],
      '/^(margin|padding|gap|row-gap|column-gap|inset)(-[a-z-]+)?$/': [new RegExp(`^${space}( ${space})*$`)],
      '/^(border|outline)(-(top|right|bottom|left|block|inline)(-(start|end))?)?$/': [/^(0|none|var\(--line(-[a-z]+)?\))$/],
    },
  },
  overrides: [
    {
      files: ['src/styles/tokens.css'],
      rules: {
        'color-no-hex': null,
        'color-named': null,
        'function-disallowed-list': null,
        'property-disallowed-list': null,
        'declaration-property-value-allowed-list': null,
      },
    },
  ],
}
