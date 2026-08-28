import DefaultTheme from 'vitepress/theme';
import { h } from 'vue';
import type { Theme } from 'vitepress';
import { enhanceAppWithTabs } from 'vitepress-plugin-tabs/client';
import HeroHeadline from './HeroHeadline.vue';
import HomeBirdReveal from './HomeBirdReveal.vue';
import HomeEngine from './HomeEngine.vue';
import HomeEtymology from './HomeEtymology.vue';
import SiteLogo from './SiteLogo.vue';
import './custom.css';

export default {
  extends: DefaultTheme,
  Layout: () => {
    return h(DefaultTheme.Layout, null, {
      'nav-bar-title-before': () => h(SiteLogo),
      'home-hero-info': () => h(HeroHeadline),
      'home-hero-after': () => [
        h(HomeBirdReveal),
        h(HomeEtymology),
        h(HomeEngine),
      ],
    });
  },
  enhanceApp({ app }) {
    enhanceAppWithTabs(app);
  },
} satisfies Theme;
