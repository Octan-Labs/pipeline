import { BadRequestException } from '@nestjs/common';

// https://stackoverflow.com/a/66534018/10265299
type EnumKeys<Enum> = Exclude<keyof Enum, number>;

const _enumObject = <Enum extends Record<string, number | string>>(e: Enum) => {
  const copy = { ...e } as { [K in EnumKeys<Enum>]: Enum[K] };
  Object.values(e).forEach(
    (value) => typeof value === 'number' && delete copy[value]
  );
  return copy;
};

export const enumKeys = <Enum extends Record<string, number | string>>(
  e: Enum
) => {
  return Object.keys(_enumObject(e)) as EnumKeys<Enum>[];
};

export const enumValues = <Enum extends Record<string, number | string>>(
  e: Enum
) => {
  return [...new Set(Object.values(_enumObject(e)))] as Enum[EnumKeys<Enum>][];
};

export const checkContainNullFields = (object: any) => {
  for (const [key, value] of Object.entries(object)) {
    if (!value) {
      throw new BadRequestException(`${key} field is missing from DB!`);
    }
  }
};
// https://stackoverflow.com/a/66534018/10265299

export const sleep = (millisecond: number) =>
  new Promise((resolve) => setTimeout(resolve, millisecond));
