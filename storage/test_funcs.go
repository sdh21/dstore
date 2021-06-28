package storage

// TODO: temporary function
func (sg *Storage) clearCachedBlocks() error {
	for _, block := range sg.cachedOpenedFiles {
		err := sg.cacheCloseBlock(block)
		if err != nil {
			return err
		}
	}
	sg.cachedOpenedFiles = make(map[string]*OpenedFile)
	return nil
}
