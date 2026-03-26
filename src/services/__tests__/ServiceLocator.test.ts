import { SERVICE_NAMES, ServiceLocator } from '../ServiceLocator';

describe('ServiceLocator', () => {
  let locator: ServiceLocator;

  beforeEach(() => {
    locator = new ServiceLocator();
  });

  describe('register and get', () => {
    it('should register and retrieve a service', () => {
      const mockService = { name: 'test' };
      locator.register('test', () => mockService);

      const result = locator.get('test');

      expect(result).toBe(mockService);
    });

    it('should create service lazily (only when first accessed)', () => {
      const factory = jest.fn().mockReturnValue({ name: 'lazy' });
      locator.register('lazy', factory);

      // Factory should not be called yet
      expect(factory).not.toHaveBeenCalled();

      // Now access the service
      locator.get('lazy');
      expect(factory).toHaveBeenCalledTimes(1);

      // Subsequent access should not call factory again (singleton)
      locator.get('lazy');
      expect(factory).toHaveBeenCalledTimes(1);
    });

    it('should return cached instance on subsequent calls (singleton)', () => {
      let callCount = 0;
      locator.register('counter', () => ({ id: ++callCount }));

      const first = locator.get<{ id: number }>('counter');
      const second = locator.get<{ id: number }>('counter');

      expect(first).toBe(second);
      expect(first.id).toBe(1);
    });

    it('should throw error for unregistered service', () => {
      expect(() => locator.get('nonexistent')).toThrow(
        "Service 'nonexistent' is not registered",
      );
    });

    it('should throw error when registering duplicate service', () => {
      locator.register('dup', () => ({}));

      expect(() => locator.register('dup', () => ({}))).toThrow(
        "Service 'dup' is already registered",
      );
    });
  });

  describe('registerInstance', () => {
    it('should register and retrieve a pre-created instance', () => {
      const instance = { preCreated: true };
      locator.registerInstance('preCreated', instance);

      const result = locator.get('preCreated');

      expect(result).toBe(instance);
    });

    it('should throw error when registering duplicate instance', () => {
      locator.registerInstance('dup', {});

      expect(() => locator.registerInstance('dup', {})).toThrow(
        "Service 'dup' is already registered",
      );
    });
  });

  describe('circular dependency detection', () => {
    it('should detect direct circular dependency', () => {
      locator.register('a', () => locator.get('a'));

      expect(() => locator.get('a')).toThrow(
        "Circular dependency detected while resolving 'a'",
      );
    });

    it('should detect indirect circular dependency', () => {
      locator.register('a', () => locator.get('b'));
      locator.register('b', () => locator.get('c'));
      locator.register('c', () => locator.get('a'));

      expect(() => locator.get('a')).toThrow('Circular dependency detected');
    });

    it('should allow lazy references that break circular deps', () => {
      // Service A has a method that uses B, but doesn't need B at construction
      interface ServiceA {
        name: string;
        getB: () => ServiceB;
      }
      interface ServiceB {
        name: string;
      }

      locator.register<ServiceA>('a', () => ({
        name: 'A',
        getB: () => locator.get<ServiceB>('b'), // Lazy reference
      }));
      locator.register<ServiceB>('b', () => ({
        name: 'B',
      }));

      // Both services can be created
      const a = locator.get<ServiceA>('a');
      const b = locator.get<ServiceB>('b');

      expect(a.name).toBe('A');
      expect(b.name).toBe('B');
      expect(a.getB()).toBe(b);
    });
  });

  describe('has', () => {
    it('should return true for registered factory', () => {
      locator.register('test', () => ({}));
      expect(locator.has('test')).toBe(true);
    });

    it('should return true for registered instance', () => {
      locator.registerInstance('test', {});
      expect(locator.has('test')).toBe(true);
    });

    it('should return false for unregistered service', () => {
      expect(locator.has('nonexistent')).toBe(false);
    });
  });

  describe('getRegisteredServices', () => {
    it('should return sorted list of all registered services', () => {
      locator.register('zebra', () => ({}));
      locator.register('alpha', () => ({}));
      locator.registerInstance('beta', {});

      const services = locator.getRegisteredServices();

      expect(services).toEqual(['alpha', 'beta', 'zebra']);
    });
  });

  describe('clearInstances', () => {
    it('should clear cached instances but keep factories', () => {
      let callCount = 0;
      locator.register('counter', () => ({ id: ++callCount }));

      locator.get('counter'); // id = 1
      expect(callCount).toBe(1);

      locator.clearInstances();

      const result = locator.get<{ id: number }>('counter');
      expect(callCount).toBe(2);
      expect(result.id).toBe(2);
    });
  });

  describe('clear', () => {
    it('should clear everything', () => {
      locator.register('test', () => ({}));
      locator.get('test');

      locator.clear();

      expect(locator.has('test')).toBe(false);
      expect(locator.getRegisteredServices()).toEqual([]);
    });
  });

  describe('SERVICE_NAMES', () => {
    it('should have all expected service names', () => {
      expect(SERVICE_NAMES.Logger).toBe('logger');
      expect(SERVICE_NAMES.Dbt).toBe('dbt');
      expect(SERVICE_NAMES.Framework).toBe('framework');
      expect(SERVICE_NAMES.Api).toBe('api');
      expect(SERVICE_NAMES.Lightdash).toBe('lightdash');
    });
  });
});
