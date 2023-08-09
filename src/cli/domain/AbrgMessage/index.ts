import i18next from 'i18next';
import { getSystemLocale } from './getSystemLocale';
export * from './getSystemLocale';

i18next.init({
  fallbackLng: 'en',
  resources: {
    en: {
      translation: require('./locales/en.ts').default,
    },
    ja: {
      translation: require('./locales/ja.ts').default,
    }
  }
})

export enum AbrgMessage {
  CLI_COMMON_DATADIR_OPTION = 'CLI_COMMON_WORKDIR_OPTION',
  CLI_COMMON_RESOURCE_OPTION = 'CLI_COMMON_SOURCE_OPTION',

  CLI_UPDATE_CHECK_DESC = 'CLI_UPDATE_CHECK_DESC',
  CLI_DOWNLOAD_DESC = 'CLI_DOWNLOAD_DESC',

  CLI_GEOCODE_DESC = 'CLI_GEOCODE_DESC',
  CLI_GEOCODE_FUZZY_OPTION = 'CLI_GEOCODE_FUZZY_OPTION',
  CLI_GEOCODE_INPUT_FILE = 'CLI_GEOCODE_INPUT_FILE',
  CLI_GEOCODE_OUTPUT_FILE = "CLI_GEOCODE_OUTPUT_FILE",
  CLI_GEOCODE_FORMAT_OPTION = 'CLI_GEOCODE_FORMAT_OPTION',
  APPLICATION_DESC = 'APPLICATION_DESC',
  ERROR_NO_UPDATE_IS_AVAILABLE = "ERROR_NO_UPDATE_IS_AVAILABLE",
  CHECKING_UPDATE = "CHECKING_UPDATE",
  START_DOWNLOADING_NEW_DATASET = "START_DOWNLOADING_NEW_DATASET",
  EXTRACTING_THE_DATA = "EXTRACTING_THE_DATA",
  LOADING_INTO_DATABASE = "LOADING_INTO_DATABASE",
  NEW_DATASET_IS_AVAILABLE = "NEW_DATASET_IS_AVAILABLE",
  DATA_DOWNLOAD_ERROR = "DATA_DOWNLOAD_ERROR",
  CANNOT_FIND_THE_SPECIFIED_RESOURCE = "CANNOT_FIND_THE_SPECIFIED_RESOURCE",
  DOWNLOADED_DATA_DOES_NOT_CONTAIN_THE_RESOURCE_CSV = "DOWNLOADED_DATA_DOES_NOT_CONTAIN_THE_RESOURCE_CSV",
  START_DOWNLOADING = "START_DOWNLOADING",
  CANNOT_FIND_INPUT_FILE = "CANNOT_FIND_INPUT_FILE",
  INPUT_SOURCE_FROM_STDIN_ERROR = "INPUT_SOURCE_FROM_STDIN_ERROR"
};

export namespace AbrgMessage {
  let locale = getSystemLocale();
  let originalTranslater = i18next.getFixedT(locale);

  export function setLocale(locale: 'en' | 'ja') {
    originalTranslater = i18next.getFixedT(locale);
  }

  export function toString(messageId: AbrgMessage): string {
    return originalTranslater(messageId);
  }
}